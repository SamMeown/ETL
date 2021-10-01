from typing import Iterable, Set
from datetime import datetime, timedelta
import uuid
from os import environ
import json

import requests

from postgres_to_es.backoff import backoff
from postgres_to_es.models import FilmWork, NamedItem
from postgres_to_es.config import config


class Loader:
    """Класс для загрузки данных в Elasticsearch"""

    def __init__(self, dsn):
        self.dsn = dict(dsn)

    def load(self, filmworks: Iterable[FilmWork]) -> (bool, datetime):
        try:
            return self.load_impl(filmworks)
        except requests.exceptions.ConnectionError as es_connection_error:
            print('Failed to load batch to Elasticsearch:', es_connection_error)
            return self.load(filmworks)

    @backoff(start_sleep_time=config.es_db.min_backoff_delay, border_sleep_time=config.es_db.max_backoff_delay)
    def load_impl(self, filmworks: Iterable[FilmWork]) -> (bool, datetime):
        if not filmworks:
            print('Loading to Elasticsearch: empty list')
            return True, None

        bulk_request_string = self.transform_films_to_raw_request_data(filmworks)
        headers = {'Content-Type': 'application/x-ndjson'}
        response = requests.post(f"http://{self.dsn['host']}:{self.dsn['port']}/_bulk?filter_path=errors",
                                 data=bulk_request_string,
                                 headers=headers)

        if response.status_code != 200 or response.json().get('errors', True) is True:
            print(f'Loading to Elasticsearch: loaded with errors ({response.status_code})')
            return False, None

        print(f'Loading to Elasticsearch: success ({response.status_code})')

        return True, filmworks[-1].updated_at

    def transform_films_to_raw_request_data(self, filmworks: Iterable[FilmWork]) -> str:
        bulk_request_data = []
        for filmwork in filmworks:
            if not filmwork.title:
                bulk_request_data.append(
                    {"delete": {"_index": self.dsn['dbname'], "_id": str(filmwork.id)}}
                )
            else:
                bulk_request_data.append(
                    {"index": {"_index": self.dsn['dbname'], "_id": str(filmwork.id)}}
                )
                bulk_request_data.append(
                    {
                        "actors": self.named_items_array(filmwork.actors),
                        "actors_names": self.named_items_names(filmwork.actors),
                        "writers": self.named_items_array(filmwork.writers),
                        "writers_names": self.named_items_names(filmwork.writers),
                        "directors": self.named_items_array(filmwork.directors),
                        "directors_names": self.named_items_names(filmwork.directors),
                        "genres": self.named_items_array(filmwork.genres),
                        "genres_names": self.named_items_names(filmwork.genres),
                        "title": filmwork.title,
                        "description": filmwork.description,
                        "imdb_rating": filmwork.rating,
                        "id": str(filmwork.id)
                    }
                )
        bulk_request_string = '\n'.join(json.dumps(data) for data in bulk_request_data) + '\n'

        return bulk_request_string

    @staticmethod
    def named_items_array(named_items: Set[NamedItem]):
        return [{"id": str(item.id), "name": item.name} for item in named_items]

    @staticmethod
    def named_items_names(named_items: Set[NamedItem]):
        return ', '.join(item.name for item in named_items)
