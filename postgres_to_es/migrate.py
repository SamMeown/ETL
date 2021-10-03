import requests
import logging

from postgres_to_es.config import config
from postgres_to_es.es_db_schema import db_schema


def create_index():
    try:
        headers = {'Content-Type': 'application/json'}
        response = requests.put('http://{host}:{port}/{path}'.format(host=config.es_db.dsn.host,
                                                                     port=config.es_db.dsn.port,
                                                                     path=config.es_db.dsn.dbname),
                                data=db_schema,
                                headers=headers)
        logging.info(f'Finished with response: {response.status_code} ({response.text}))')
    except requests.exceptions.ConnectionError as es_connection_error:
        logging.warning(f'Failed to connect to ES: {es_connection_error}', )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s : %(name)s - %(levelname)s - %(message)s')
    create_index()
