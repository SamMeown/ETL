import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, date
import uuid
import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import execute_batch, register_uuid, DictCursor
from enum import Enum, auto
from typing import List


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

    def save_genres(self, genres: List[Genre]):
        """Запись жанров в таблицу content.genre"""
        with self.pg_conn.cursor() as cursor:
            execute_batch(
                cursor,
                "INSERT INTO content.genre (id, name, description, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)",
                ((genre.id, genre.name, genre.description, genre.created_at, genre.updated_at) for genre in genres),
                page_size=100
            )

    def save_persons(self, persons: List[Person]):
        """Запись персон в таблицу content.person"""
        with self.pg_conn.cursor() as cursor:
            execute_batch(
                cursor,
                """
                INSERT INTO content.person (id, full_name, birth_date, created_at, updated_at) 
                VALUES (%s, %s, %s, %s, %s)
                """,
                ((person.id, person.full_name, person.birth_date, person.created_at, person.updated_at)
                 for person in persons),
                page_size=1000
            )

    def save_film_works(self, film_works: List[FilmWork]):
        """Запись фильмов в таблицу content.film_work"""
        with self.pg_conn.cursor() as cursor:
            execute_batch(
                cursor,
                """
                INSERT INTO content.film_work 
                (id, title, description, creation_date, certificate, file_path, rating, type, created_at, updated_at)
                VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                ((film_work.id, film_work.title, film_work.description, film_work.creation_date, film_work.certificate,
                  film_work.file_path, film_work.rating, film_work.type.name, film_work.created_at,
                  film_work.updated_at) for film_work in film_works),
                page_size=500
            )

    def save_genre_film_works(self, genre_film_works: List[GenreFilmWork]):
        """Запись жанров фильмов в таблицу content.genre_film_work"""
        with self.pg_conn.cursor() as cursor:
            execute_batch(
                cursor,
                """
                INSERT INTO content.genre_film_work (id, film_work_id, genre_id, created_at)
                VALUES (%s, %s, %s, %s)
                """,
                ((genre_film_work.id, genre_film_work.film_work_id, genre_film_work.genre_id,
                  genre_film_work.created_at) for genre_film_work in genre_film_works),
                page_size=1000
            )

    def save_person_film_works(self, person_film_works: List[PersonFilmWork]):
        """Запись ролей персон в фильмах в таблицу content.person_film_work"""
        with self.pg_conn.cursor() as cursor:
            execute_batch(
                cursor,
                """
                INSERT INTO content.person_film_work (id, film_work_id, person_id, role, created_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                ((person_film_work.id, person_film_work.film_work_id, person_film_work.person_id,
                  person_film_work.role.name, person_film_work.created_at) for person_film_work in person_film_works),
                page_size=1000
            )


class SQLiteLoader:
    """Класс для загрузки данных из sqlite"""
    def __init__(self, conn):
        self.conn = conn
        sqlite3.register_converter("timestamp", SQLiteLoader.convert_timestamp)

    def load_genres(self) -> List[Genre]:
        """Загрузка жанров из таблицы genre"""
        cursor = self.conn.cursor()
        cursor.execute("""
                SELECT id, name, description, created_at, updated_at
                FROM genre
            """)
        data = [Genre(id=uuid.UUID(row[0]),
                      name=row[1],
                      description=row[2],
                      created_at=row[3],
                      updated_at=row[4]) for row in cursor.fetchall()]
        cursor.close()

        return data

    def load_persons(self) -> List[Person]:
        """Загрузка персон из таблицы person"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT id, full_name, birth_date, created_at, updated_at
            FROM person
        """)
        data = [Person(id=uuid.UUID(row[0]),
                       full_name=row[1],
                       birth_date=row[2],
                       created_at=row[3],
                       updated_at=row[4]) for row in cursor.fetchall()]
        cursor.close()

        return data

    def load_film_works(self) -> List[FilmWork]:
        """Загрузка фильмов из таблицы film_work"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT id, title, description, creation_date, certificate, file_path, rating, type, created_at, updated_at
            FROM film_work
        """)
        data = [FilmWork(id=uuid.UUID(row[0]),
                         title=row[1],
                         description=row[2],
                         creation_date=row[3],
                         certificate=row[4],
                         file_path=row[5],
                         rating=row[6],
                         type=FilmWorkType[row[7]],
                         created_at=row[8],
                         updated_at=row[9]) for row in cursor.fetchall()]
        cursor.close()

        return data

    def load_person_film_works(self) -> List[PersonFilmWork]:
        """Загрузка ролей персон в фильмах из таблицы person_film_work"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT id, film_work_id, person_id, role, created_at
            FROM person_film_work
        """)
        data = [PersonFilmWork(id=uuid.UUID(row[0]),
                               film_work_id=uuid.UUID(row[1]),
                               person_id=uuid.UUID(row[2]),
                               role=PersonFilmWorkRole[row[3]],
                               created_at=row[4]) for row in cursor.fetchall()]
        cursor.close()

        return data

    def load_genre_film_works(self) -> List[GenreFilmWork]:
        """Загрузка жанров фильмов из таблицы genre_film_work"""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT id, film_work_id, genre_id, created_at
            FROM genre_film_work
        """)
        data = [GenreFilmWork(id=uuid.UUID(row[0]),
                              film_work_id=uuid.UUID(row[1]),
                              genre_id=uuid.UUID(row[2]),
                              created_at=row[3]) for row in cursor.fetchall()]
        cursor.close()

        return data

    @staticmethod
    def convert_timestamp(val):
        """Слегка модифицированный конвертер для чтения timestamp в datetime,
        который не падает при размере поля с микросекундами менее шести символов.
        """
        datepart, timepart = val.split(b" ")
        year, month, day = map(int, datepart.split(b"-"))
        timepart_full = timepart.split(b".")
        hours, minutes, seconds = map(int, timepart_full[0].split(b":"))
        if len(timepart_full) == 2:
            microseconds = int('{:0<6.6}'.format(
                timepart_full[1].decode().replace('+', ' ', 1).replace('-', ' ', 1).replace('Z', ' ', 1).split(' ', 1)[
                    0]))
        else:
            microseconds = 0

        val = datetime(year, month, day, hours, minutes, seconds, microseconds)
        return val


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(connection)

    # Предварительно очищаем все таблицы в postgres
    postgres_saver.truncate_all()

    # По очереди считываем данные из каждой таблицы sqlite и записываем в postgres
    data = sqlite_loader.load_genres()
    postgres_saver.save_genres(data)

    data = sqlite_loader.load_persons()
    postgres_saver.save_persons(data)

    data = sqlite_loader.load_film_works()
    postgres_saver.save_film_works(data)

    data = sqlite_loader.load_person_film_works()
    postgres_saver.save_person_film_works(data)

    data = sqlite_loader.load_genre_film_works()
    postgres_saver.save_genre_film_works(data)


if __name__ == '__main__':
    dsn = {
        'dbname': 'movies',
        'user': 'postgres',
        'password': 'SomeBigSecret',
        'host': '127.0.0.1',
        'port': 5432
    }

    db_name = 'db.sqlite'

    with closing(sqlite3.connect(db_name, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)) as sqlite_conn,\
            psycopg2.connect(**dsn, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
