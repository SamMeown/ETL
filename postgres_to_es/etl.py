import time

from postgres_to_es.state_storage import JsonFileStorage, State
from postgres_to_es.config import config
from postgres_to_es.extractor import Extractor, ExtractorState
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
        synced_state = ExtractorState.fromisoformat([state.get_state('filmworks_synced_date'),
                                                     state.get_state('persons_synced_date'),
                                                     state.get_state('genres_synced_date')])

        extract_res = extractor.extract_batch(synced_state)
        if not extract_res.filmworks and not extract_res.state:
            print('ETL: Nothing more to sync')
            break

        load_result = True
        if extract_res.filmworks:
            print(f'ETL: Extracted {len(extract_res.filmworks)} filmworks')
            load_result, _ = loader.load(extract_res.filmworks)
            if load_result:
                print(f'ETL: Loaded {len(extract_res.filmworks)} filmworks')
        if extract_res.state and load_result:
            state.set_state('filmworks_synced_date', extract_res.state.filmworks_state.isoformat())
            state.set_state('persons_synced_date', extract_res.state.persons_state.isoformat())
            state.set_state('genres_synced_date', extract_res.state.genres_state.isoformat())


if __name__ == '__main__':
    sync_es_with_postgres()
