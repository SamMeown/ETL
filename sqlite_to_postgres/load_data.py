import sqlite3
from contextlib import closing
from dataclasses import dataclass, astuple, fields
from typing import Iterable
from datetime import datetime, date
import uuid
from enum import Enum, auto
from os import environ

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import execute_batch, register_uuid, DictCursor


@dataclass(frozen=True)
class Genre:
    id: uuid.UUID
    name: str
    description: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class Person:
    id: uuid.UUID
    full_name: str
    birth_date: date
    created_at: datetime
    updated_at: datetime


class FilmWorkType(Enum):
    movie = auto()
    tv_show = auto()


@dataclass(frozen=True)
class FilmWork:
    id: uuid.UUID
    title: str
    description: str
    creation_date: date
    certificate: str
    file_path: str
    rating: float
    type: FilmWorkType
    created_at: datetime
    updated_at: datetime


class PersonFilmWorkRole(Enum):
    actor = auto()
    director = auto()
    writer = auto()


@dataclass(frozen=True)
class PersonFilmWork:
    id: uuid.UUID
    film_work_id: uuid.UUID
    person_id: uuid.UUID
    role: PersonFilmWorkRole
    created_at: datetime


@dataclass(frozen=True)
class GenreFilmWork:
    id: uuid.UUID
    film_work_id: uuid.UUID
    genre_id: uuid.UUID
    created_at: datetime


@dataclass(frozen=True)
class TableInfo:
    table_name: str
    data_type: type


tables = (
    TableInfo(table_name='genre', data_type=Genre),
    TableInfo(table_name='person', data_type=Person),
    TableInfo(table_name='film_work', data_type=FilmWork),
    TableInfo(table_name='person_film_work', data_type=PersonFilmWork),
    TableInfo(table_name='genre_film_work', data_type=GenreFilmWork),
)


class PostgresSaver:
    """Класс для записи данных в postgres"""
    def __init__(self, pg_conn):
        self.pg_conn = pg_conn
        register_uuid()

    def truncate_all(self):
        """Метод для очистки всех таблиц"""
        with self.pg_conn.cursor() as cursor:
            cursor.execute("""TRUNCATE content.genre, content.person, content.film_work CASCADE""")
            cursor.execute("""TRUNCATE content.person_film_work, content.genre_film_work""")

    @staticmethod
    def raw_data_from_item(item):
        return map(lambda value: value.name if issubclass(type(value), Enum) else value,
                   astuple(item))

    def save_items(self, db, items, page_size=1000):
        """Запись айтемов items в таблицу content.db"""
        if not items:
            return

        with self.pg_conn.cursor() as cursor:
            item_type = type(items[0])
            fields_list = ", ".join([fld.name for fld in fields(item_type)])
            placeholder_list = ", ".join(["%s"] * len(fields(item_type)))
            sql_request = f"""
                            INSERT INTO content.{db} 
                            ({fields_list})
                            VALUES 
                            ({placeholder_list})
                        """
            execute_batch(
                cursor,
                sql_request,
                (list(self.raw_data_from_item(item)) for item in items),
                page_size=page_size
            )


class SQLiteLoader:
    """Класс для загрузки данных из sqlite"""
    def __init__(self, conn):
        self.conn = conn
        sqlite3.register_converter("timestamp", SQLiteLoader.convert_timestamp)

    @staticmethod
    def item_from_raw_data(raw_data, item_type):
        item_field_values = []
        for value, value_field in zip(raw_data, fields(item_type)):
            adapted_value = value
            value_type = value_field.type
            if issubclass(value_type, uuid.UUID):
                adapted_value = value_type(value)
            elif issubclass(value_type, Enum):
                adapted_value = value_type[value]
            item_field_values.append(adapted_value)

        return item_type(*item_field_values)

    def load_items(self, db, item_type):
        """Загрузка данных типа item_type из таблицы db"""
        cursor = self.conn.cursor()

        fields_list = ", ".join([fld.name for fld in fields(item_type)])
        sql_request = f"""
                SELECT {fields_list}
                FROM {db}
            """
        cursor.execute(sql_request)
        data = [self.item_from_raw_data(raw_data, item_type) for raw_data in cursor.fetchall()]
        cursor.close()

        return data

    @staticmethod
    def convert_timestamp(db_timestamp):
        """Слегка модифицированный конвертер для чтения timestamp в datetime,
        который не падает при размере поля с микросекундами менее шести символов.
        """
        datepart, timepart = db_timestamp.split(b" ")
        year, month, day = map(int, datepart.split(b"-"))
        timepart_full = timepart.split(b".")
        hours, minutes, seconds = map(int, timepart_full[0].split(b":"))
        if len(timepart_full) == 2:
            microseconds = int('{:0<6.6}'.format(
                timepart_full[1].decode().replace('+', ' ', 1).replace('-', ' ', 1).replace('Z', ' ', 1).split(' ', 1)[
                    0]))
        else:
            microseconds = 0

        converted_timestamp = datetime(year, month, day, hours, minutes, seconds, microseconds)
        return converted_timestamp


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection, table_infos: Iterable[TableInfo]):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(connection)

    # Предварительно очищаем все таблицы в postgres
    postgres_saver.truncate_all()

    # По очереди считываем данные из каждой таблицы sqlite и записываем в postgres
    for table_info in table_infos:
        data = sqlite_loader.load_items(table_info.table_name, table_info.data_type)
        postgres_saver.save_items(table_info.table_name, data)


if __name__ == '__main__':
    dsn = {
        'dbname': environ.get('MIGRATION_DST_DB_NAME'),
        'user': environ.get('MIGRATION_DST_DB_USER'),
        'password': environ.get('MIGRATION_DST_DB_PASSWORD'),
        'host': environ.get('MIGRATION_DST_DB_HOST', '127.0.0.1'),
        'port': int(environ.get('MIGRATION_DST_DB_PORT', '5432'))
    }

    db_path = environ.get('MIGRATION_SRC_DB_PATH', 'db.sqlite')

    with closing(sqlite3.connect(db_path,
                                 detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)) as sqlite_connection,\
            psycopg2.connect(**dsn,
                             cursor_factory=DictCursor) as pg_connection:
        load_from_sqlite(sqlite_connection, pg_connection, tables)
