from typing import Iterable
from datetime import datetime
from os import environ

import psycopg2
from psycopg2.extras import register_uuid, DictCursor

from postgres_to_es.backoff import backoff
from postgres_to_es.models import FilmWork, NamedItem
from postgres_to_es.config import config


class Extractor:
    """Класс для выгрузки данных из PostgreSQL пачками"""

    def __init__(self, dsn, batch_size):
        register_uuid()
        self.dsn = dict(dsn)
        self.batch_size = batch_size or 100
        self.connection = None
        self.connect()

    def __del__(self):
        if self.connection:
            self.connection.close()

    def connect(self):
        try:
            self.connect_impl()
        except psycopg2.OperationalError as db_exception:
            print('Failed to connect to postgres:', db_exception)
            self.connect()

    @backoff(start_sleep_time=config.postgres_db.min_backoff_delay,
             border_sleep_time=config.postgres_db.max_backoff_delay)
    def connect_impl(self):
        self.connection = psycopg2.connect(**self.dsn, cursor_factory=DictCursor)

    def extract_batch(self, extract_since=None) -> Iterable[FilmWork]:
        if not extract_since:
            extract_since = datetime.min
        try:
            return self.extract_batch_impl(extract_since)
        except psycopg2.OperationalError as db_exception:
            print('Failed to execute extract_batch from postgres:', db_exception)
            self.connect()
            return self.extract_batch(extract_since)

    def extract_batch_impl(self, extract_since) -> Iterable[FilmWork]:
        with self.connection as connection:
            with connection.cursor() as cursor:
                # Тут есть один скользкий момент, который мы постараемся учесть. Дело в том, что при bulk_update записей
                # в таблице content.etl_updates всем апдейченым записям проставляется в точности одно и то же время
                # updated_at. Поэтому, если обновится сразу много записей, больше размера читаемой пачки - такое может
                # быть в случае обновления одного из жанров - мы можем попасть в бесконечный цикл, читая каждый раз одну
                # и ту же пачку без возможности выбраться из данного updated_at, либо, если мы, чтобы не застревать,
                # инкрементим updated_at на минимальную дельту после успешного load'a пачки - будем пропускать часть
                # обновленных данных. Вобщем нам надо иметь ввиду, что пачка может иногда получаться больше желаемого
                # размера. Для определения пачки используем такой алгоритм:
                #
                # 1. Определяем пачку данных заданного в настройках размера, сортируя и обрезая таблицу нужным образом
                # 2. Смотрим, какой updated_at у последней записи в пачке и расширяем пачку на все оставшиеся в таблице
                #    записи с таким updated_at.
                #
                # Это можно делать за один или за два запроса. За один запрос можно так:
                #
                # sql_request = f"""
                #                     SELECT film_work_id
                #                     FROM content.etl_updates
                #                     WHERE updated_at >= '{extract_since.isoformat(sep=" ")}'
                #                         AND updated_at <= (SELECT MAX(updated_at)
                #                                            FROM (SELECT updated_at
                #                                                  FROM content.etl_updates
                #                                                  WHERE updated_at >= '{extract_since.isoformat(sep=" ")}'
                #                                                  ORDER BY updated_at
                #                                                  LIMIT {self.batch_size}) as intermediate)
                #                     ORDER BY updated_at;
                #                                 """
                # Это по идее быстрее, но наверное запрос не очень читабельный и поэтому может быть трудно
                # поддерживаемый(?). Поэтому дальше я сделал за два запроса)

                sql_request = f"""
                                SELECT updated_at
                                FROM content.etl_updates
                                WHERE updated_at >= '{extract_since.isoformat(sep=" ")}'
                                ORDER BY updated_at
                                LIMIT {self.batch_size};
                            """

                cursor.execute(sql_request)
                update_dates = cursor.fetchall()

                if not update_dates:
                    return []

                latest_update_date = update_dates[-1][0]
                sql_request = f"""
                                SELECT film_work_id
                                FROM content.etl_updates
                                WHERE updated_at BETWEEN '{extract_since.isoformat(sep=" ")}' 
                                                          AND '{latest_update_date.isoformat(sep=" ")}'
                                ORDER BY updated_at;
                            """

                cursor.execute(sql_request)
                films = cursor.fetchall()
                film_ids = [f"'{film[0]}'" for film in films]

                sql_request_2 = f"""
                                   SELECT
                                       eu.film_work_id, 
                                       fw.title, 
                                       fw.description,
                                       fw.rating, 
                                       fw.type,
                                       eu.updated_at,  
                                       pfw.role, 
                                       p.id, 
                                       p.full_name,
                                       g.id,
                                       g.name
                                   FROM content.etl_updates as eu
                                   LEFT JOIN content.film_work as fw ON eu.film_work_id = fw.id
                                   LEFT JOIN content.person_film_work as pfw ON pfw.film_work_id = fw.id
                                   LEFT JOIN content.person as p ON p.id = pfw.person_id
                                   LEFT JOIN content.genre_film_work as gfw ON gfw.film_work_id = fw.id
                                   LEFT JOIN content.genre as g ON g.id = gfw.genre_id
                                   WHERE eu.film_work_id IN ({', '.join(film_ids)})
                                   ORDER BY eu.updated_at, eu.film_work_id;
                                   
                           """
                cursor.execute(sql_request_2)

                return self.transform_raw_data_to_films(cursor.fetchall())

    @staticmethod
    def transform_raw_data_to_films(raw_data) -> Iterable[FilmWork]:
        films_data = []
        film_data = None
        for data in raw_data:
            if not films_data or data[0] != films_data[-1].id:
                film_data = FilmWork(id=data[0], title=data[1], description=data[2], rating=data[3],
                                     type=data[4], updated_at=data[5])
                films_data.append(film_data)
            if data[6]:
                person = NamedItem(id=data[7], name=data[8])
                persons = getattr(film_data, {'actor': 'actors',
                                              'writer': 'writers',
                                              'director': 'directors'}.get(data[6]))
                persons.add(person)
            if data[9]:
                genre = NamedItem(id=data[9], name=data[10])
                film_data.genres.add(genre)

        return films_data
