import requests

from postgres_to_es.config import config
from postgres_to_es.es_db_schema import db_schema


def create_index():
    try:
        headers = {'Content-Type': 'application/json'}
        response = requests.put(f"http://{config.es_db.dsn.host}:{config.es_db.dsn.port}/{config.es_db.dsn.dbname}",
                                data=db_schema,
                                headers=headers)
        print('Finished with response:', response.status_code, response.text)
    except requests.exceptions.ConnectionError as es_connection_error:
        print('Failed to connect to ES:', es_connection_error)


if __name__ == "__main__":
    create_index()
