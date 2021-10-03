from typing import Iterable, List, Any
from datetime import datetime
from os import environ
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import pytz
import logging

import psycopg2
from psycopg2.extras import register_uuid, DictCursor

from postgres_to_es.backoff import backoff
from postgres_to_es.models import FilmWork, NamedItem
from postgres_to_es.config import config


@dataclass
class RawRequest:
    sql_template: str = ''
    data: List[Any] = field(default_factory=list)


@dataclass
class ExtractorState:
    filmworks_state: datetime
    persons_state: datetime
    genres_state: datetime

    @classmethod
    def fromisoformat(cls, iso_list: List[str]):
        if iso_list[0] is None:
            return None
        return cls(filmworks_state=datetime.fromisoformat(iso_list[0]),
                   persons_state=datetime.fromisoformat(iso_list[1]),
                   genres_state=datetime.fromisoformat(iso_list[2]))


@dataclass
class BatchExtractResult:
    filmworks: Iterable[FilmWork] = None
    state: ExtractorState = None


class BaseExtractor(ABC):
    """Базовый класс загрузчика фильмов"""
    def __init__(self, batch_size: int):
        register_uuid()
        self.batch_size = batch_size

    @abstractmethod
    def get_extract_request(self, cursor, extract_since: ExtractorState) -> RawRequest:
        pass

    def extract_batch(self, connection, extract_since: ExtractorState) -> BatchExtractResult:
        with connection:
            with connection.cursor() as cursor:
                sql_request = self.get_extract_request(cursor, extract_since)
                if not sql_request:
                    return BatchExtractResult()

                cursor.execute(sql_request.sql_template, sql_request.data)
                film_ids = [f"{film[0]}" for film in cursor]

                if not film_ids:
                    return BatchExtractResult()

                enriched_data = self.enrich(cursor, film_ids)
                transformed_data = self.transform_raw_data_to_films(enriched_data)

                return BatchExtractResult(filmworks=transformed_data,
                                          state=extract_since)

    @staticmethod
    def enrich(cursor, film_ids: List[str]) -> Iterable:
        sql_request = f"""
                           SELECT
                               fw.id, 
                               fw.title, 
                               fw.description,
                               fw.rating, 
                               fw.type,
                               fw.updated_at,  
                               pfw.role as p_role, 
                               p.id as p_id, 
                               p.full_name as p_full_name,
                               p.updated_at as p_updated_at,
                               g.id as g_id,
                               g.name as g_name,
                               g.updated_at as g_updated_at
                           FROM content.film_work as fw 
                           LEFT JOIN content.person_film_work as pfw ON pfw.film_work_id = fw.id
                           LEFT JOIN content.person as p ON p.id = pfw.person_id
                           LEFT JOIN content.genre_film_work as gfw ON gfw.film_work_id = fw.id
                           LEFT JOIN content.genre as g ON g.id = gfw.genre_id
                           WHERE fw.id IN %s
                           ORDER BY fw.updated_at, fw.id;

                           """
        cursor.execute(sql_request, (tuple(film_ids),))

        return cursor.fetchall()

    @staticmethod
    def transform_raw_data_to_films(raw_data) -> List[FilmWork]:
        films_data = []
        film_data = None
        for data in raw_data:
            if not films_data or data['id'] != films_data[-1].id:
                film_data = FilmWork(id=data['id'], title=data['title'], description=data['description'],
                                     rating=data['rating'], type=data['type'], updated_at=data['updated_at'])
                films_data.append(film_data)
            if data['p_role']:
                person = NamedItem(id=data['p_id'], name=data['p_full_name'], updated_at=data['p_updated_at'])
                persons = getattr(film_data, {'actor': 'actors',
                                              'writer': 'writers',
                                              'director': 'directors'}.get(data['p_role']))
                persons.add(person)
            if data['g_id']:
                genre = NamedItem(id=data['g_id'], name=data['g_name'], updated_at=data['g_updated_at'])
                film_data.genres.add(genre)

        return films_data


class FilmworksExtractor(BaseExtractor):
    def get_extract_request(self, cursor, extract_since: ExtractorState):
        request = RawRequest()
        request.sql_template = """
                                    SELECT id
                                    FROM content.film_work
                                    WHERE updated_at > %s
                                    ORDER BY updated_at
                                    LIMIT %s;
                                """
        request.data = (extract_since.filmworks_state.isoformat(sep=" "), self.batch_size)

        return request

    def extract_batch(self, connection, extract_since: ExtractorState):
        extract_res = super().extract_batch(connection, extract_since)

        if extract_res.filmworks:
            extract_res.state.filmworks_state = extract_res.filmworks[-1].updated_at

            max_person_updated_at = datetime.min.replace(tzinfo=pytz.UTC)
            max_genre_updated_at = datetime.min.replace(tzinfo=pytz.UTC)
            for filmwork in extract_res.filmworks:
                persons = (*filmwork.actors, *filmwork.writers, *filmwork.directors)
                if persons:
                    max_person_updated_at = max(max_person_updated_at,
                                                max(person.updated_at for person in persons))
                if filmwork.genres:
                    max_genre_updated_at = max(max_genre_updated_at,
                                               max(genre.updated_at for genre in filmwork.genres))

            extract_res.state.persons_state = max(extract_res.state.persons_state, max_person_updated_at)
            extract_res.state.genres_state = max(extract_res.state.genres_state, max_genre_updated_at)

        return extract_res


class FilmworksFromPersonsExtractor(BaseExtractor):

    person_ids: List[str] = None
    filmworks_extract_since: datetime = datetime.min
    max_persons_updated_at: datetime = datetime.min

    def get_extract_request(self, cursor, extract_since: ExtractorState):
        if not self.person_ids:
            sql_request = """
                            SELECT id, updated_at
                            FROM content.person
                            WHERE updated_at > %s
                            ORDER BY updated_at
                            LIMIT %s;
                        """
            cursor.execute(sql_request, (extract_since.persons_state, self.batch_size))
            persons = cursor.fetchall()
            self.person_ids = [f"{person[0]}" for person in persons]

            if len(self.person_ids) == 0:
                return None

            self.max_persons_updated_at = persons[-1][1]

        request = RawRequest()
        request.sql_template = """
                                    SELECT DISTINCT fw.id, fw.updated_at
                                    FROM content.film_work as fw
                                    LEFT JOIN content.person_film_work as pfw ON pfw.film_work_id = fw.id
                                    WHERE pfw.person_id IN %s and fw.updated_at > %s
                                    ORDER BY fw.updated_at
                                    LIMIT %s;
                               """
        request.data = (tuple(self.person_ids), self.filmworks_extract_since.isoformat(sep=" "), self.batch_size)

        return request

    def extract_batch(self, connection, extract_since: ExtractorState):
        extract_res = super().extract_batch(connection, extract_since)
        if not extract_res.filmworks and len(self.person_ids) > 0:
            extract_res.state = extract_since
            extract_res.state.persons_state = self.max_persons_updated_at
            self.person_ids = None
        else:
            extract_res.state = None
            if extract_res.filmworks:
                self.filmworks_extract_since = extract_res.filmworks[-1].updated_at


        return extract_res


class FilmworksFromGenresExtractor(BaseExtractor):

    genre_ids: List[str] = None
    filmworks_extract_since: datetime = datetime.min
    max_genres_updated_at: datetime = datetime.min

    def get_extract_request(self, cursor, extract_since: ExtractorState):
        if not self.genre_ids:
            sql_request = """
                            SELECT id, updated_at
                            FROM content.genre
                            WHERE updated_at > %s
                            ORDER BY updated_at
                            LIMIT %s;
                        """
            cursor.execute(sql_request, (extract_since.genres_state, self.batch_size))
            genres = cursor.fetchall()
            self.genre_ids = [f"{genre[0]}" for genre in genres]

            if len(self.genre_ids) == 0:
                return None

            self.max_genres_updated_at = genres[-1][1]

        request = RawRequest()
        request.sql_template = """
                                    SELECT DISTINCT fw.id, fw.updated_at
                                    FROM content.film_work as fw
                                    LEFT JOIN content.genre_film_work as gfw ON gfw.film_work_id = fw.id
                                    WHERE gfw.genre_id IN %s and fw.updated_at > %s
                                    ORDER BY fw.updated_at
                                    LIMIT %s;
                               """
        request.data = (tuple(self.genre_ids), self.filmworks_extract_since.isoformat(sep=" "), self.batch_size)

        return request

    def extract_batch(self, connection, extract_since: ExtractorState):
        extract_res = super().extract_batch(connection, extract_since)
        if not extract_res.filmworks and len(self.genre_ids) > 0:
            extract_res.state = extract_since
            extract_res.state.genres_state = self.max_genres_updated_at
            self.genre_ids = None
        else:
            extract_res.state = None
            if extract_res.filmworks:
                self.filmworks_extract_since = extract_res.filmworks[-1].updated_at

        return extract_res


class Extractor:
    """Класс для выгрузки данных из PostgreSQL пачками"""

    def __init__(self, dsn, batch_size):
        register_uuid()
        self.dsn = dict(dsn)
        self.batch_size = batch_size or 100
        self.connection = None
        self.connect()

        self.extractors = iter((FilmworksExtractor(self.batch_size),
                                FilmworksFromPersonsExtractor(self.batch_size),
                                FilmworksFromGenresExtractor(self.batch_size)))
        self.extractor = next(self.extractors)

    def __del__(self):
        if self.connection:
            self.connection.close()

    def connect(self):
        try:
            self.connect_impl()
        except psycopg2.OperationalError as db_exception:
            logging.info('Failed to connect to postgres:', db_exception)
            self.connect()

    @backoff(start_sleep_time=config.postgres_db.min_backoff_delay,
             border_sleep_time=config.postgres_db.max_backoff_delay,
             total_sleep_time=config.postgres_db.total_backoff_time)
    def connect_impl(self):
        self.connection = psycopg2.connect(**self.dsn, cursor_factory=DictCursor)

    def extract_batch(self, extract_since=None) -> BatchExtractResult:
        if not extract_since:
            extract_since = ExtractorState(filmworks_state=datetime.min.replace(tzinfo=pytz.UTC),
                                           persons_state=datetime.min.replace(tzinfo=pytz.UTC),
                                           genres_state=datetime.min.replace(tzinfo=pytz.UTC))
        try:
            return self.extract_batch_impl(extract_since)
        except psycopg2.OperationalError as db_exception:
            logging.warning(f'Failed to execute extract_batch from postgres: {db_exception}')
            self.connect()
            return self.extract_batch(extract_since)

    def extract_batch_impl(self, extract_since) -> BatchExtractResult:
        extract_res = self.extractor.extract_batch(self.connection, extract_since)
        while not extract_res.filmworks and not extract_res.state:
            self.extractor = next(self.extractors, None)
            if not self.extractor:
                break
            extract_res = self.extractor.extract_batch(self.connection, extract_since)

        return extract_res
