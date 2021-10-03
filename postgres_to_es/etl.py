import time
import logging

from postgres_to_es.state_storage import JsonFileStorage, State
from postgres_to_es.config import config
from postgres_to_es.extractor import Extractor, ExtractorState
from postgres_to_es.loader import Loader


def sync_es_with_postgres():
    storage = JsonFileStorage(config.state_file_path)
    etl_state = State(storage)
    while True:
        logging.info('ETL: Syncing es with postgres')
        try:
            perform_etl(etl_state)
        except Exception as err:
            logging.exception(f'ETL: Failed loop iteration with error')
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
            logging.info('ETL: Nothing more to sync')
            break

        load_result = True
        if extract_res.filmworks:
            logging.info(f'ETL: Extracted {len(extract_res.filmworks)} filmworks')
            load_result, _ = loader.load(extract_res.filmworks)
            if load_result:
                logging.info(f'ETL: Loaded {len(extract_res.filmworks)} filmworks')
        if extract_res.state and load_result:
            state.set_state('filmworks_synced_date', extract_res.state.filmworks_state.isoformat())
            state.set_state('persons_synced_date', extract_res.state.persons_state.isoformat())
            state.set_state('genres_synced_date', extract_res.state.genres_state.isoformat())


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s : %(name)s - %(levelname)s - %(message)s')
    sync_es_with_postgres()
