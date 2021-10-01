from datetime import datetime, timedelta
import time

from postgres_to_es.state_storage import JsonFileStorage, State
from postgres_to_es.config import config
from postgres_to_es.extractor import Extractor
from postgres_to_es.loader import Loader


def sync_es_with_postgres():
    storage = JsonFileStorage(config.state_file_path)
    etl_state = State(storage)
    while True:
        print('ETL: Syncing es with postgres')
        perform_etl(etl_state)
        time.sleep(config.sync_interval)


def perform_etl(state: State):
    extractor = Extractor(config.postgres_db.dsn, config.batch_size)
    loader = Loader(config.es_db.dsn)

    while True:
        synced_date = state.get_state('synced_date')
        if synced_date:
            synced_date = datetime.fromisoformat(synced_date)
        filmworks = extractor.extract_batch(synced_date)
        if not filmworks:
            print('ETL: Nothing more to sync')
            break

        print(f'ETL: Extracted {len(filmworks)} filmworks')
        result, synced_date = loader.load(filmworks)
        if result and synced_date:
            print(f'ETL: Loaded {len(filmworks)} filmworks', synced_date)
            next_sync_date = synced_date + timedelta(microseconds=1)
            state.set_state('synced_date', next_sync_date.isoformat())


if __name__ == '__main__':
    sync_es_with_postgres()
