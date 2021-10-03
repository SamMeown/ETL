from typing import Iterable, Set, Dict, Optional
from datetime import datetime, timedelta
import uuid
from os import environ
import json
import logging
from http import HTTPStatus
from abc import abstractmethod, ABC

import requests

from postgres_to_es.backoff import backoff
from postgres_to_es.models import FilmWork, NamedItem
from postgres_to_es.config import config


class BaseLoader(ABC):
    """Базовый класс для загрузки данных в Elasticsearch"""

    def __init__(self, dsn):
        self.dsn = dict(dsn)

    @backoff(exceptions=(requests.exceptions.ConnectionError,),
             start_sleep_time=config.es_db.min_backoff_delay, border_sleep_time=config.es_db.max_backoff_delay,
             total_sleep_time=config.es_db.total_backoff_time)
    def load(self, filmworks: Iterable[FilmWork]) -> (bool, datetime):
        if not filmworks:
            logging.warning('Loading to Elasticsearch: empty list')
            return True, None

        bulk_request_string = self.transform_items_to_raw_request_data(filmworks)
        headers = {'Content-Type': 'application/x-ndjson'}
        response = requests.post("http://{}:{}/_bulk?filter_path=errors".format(self.dsn['host'], self.dsn['port']),
                                 data=bulk_request_string,
                                 headers=headers)

        if response.status_code != HTTPStatus.OK or response.json().get('errors', True) is True:
            logging.error(f'Loading to Elasticsearch: loaded with errors ({response.status_code})')
            return False, None

        logging.info(f'Loading to Elasticsearch: success ({response.status_code})')

        return True, filmworks[-1].updated_at

    @abstractmethod
    def transform_item_to_raw_json(self, item) -> Optional[Dict]:
        """Преобразовать входные данные в json для эластика"""
        pass

    def transform_items_to_raw_request_data(self, items: Iterable) -> str:
        bulk_request_data = []
        for item in items:
            raw_json = self.transform_item_to_raw_json(item)
            if not raw_json:
                # Удаляем
                bulk_request_data.append(
                    {"delete": {"_index": self.dsn['dbname'], "_id": str(item.id)}}
                )
            else:
                # Добавляем / обновляем
                bulk_request_data.append(
                    {"index": {"_index": self.dsn['dbname'], "_id": str(item.id)}}
                )
                bulk_request_data.append(
                    raw_json
                )
        bulk_request_string = '\n'.join(json.dumps(data) for data in bulk_request_data) + '\n'

        return bulk_request_string


class Loader(BaseLoader):
    """Класс для загрузки данных о фильмах в Elasticsearch"""

    def transform_item_to_raw_json(self, item):
        filmwork = item
        if not filmwork.title:
            return None

        return {
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

    @staticmethod
    def named_items_array(named_items: Set[NamedItem]):
        return [{"id": str(item.id), "name": item.name} for item in named_items]

    @staticmethod
    def named_items_names(named_items: Set[NamedItem]):
        return ', '.join(item.name for item in named_items)
