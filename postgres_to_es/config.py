from pydantic import BaseModel


class BaseDSNSettings(BaseModel):
    host: str
    port: int
    dbname: str


class DSNSettings(BaseDSNSettings):
    password: str
    user: str


class PostgresSettings(BaseModel):
    dsn: DSNSettings
    min_backoff_delay: float = 0.1
    max_backoff_delay: float = 5


class ElasticsearchSettings(BaseModel):
    dsn: BaseDSNSettings
    min_backoff_delay: float = 0.1
    max_backoff_delay: float = 10


class Config(BaseModel):
    postgres_db: PostgresSettings
    es_db: ElasticsearchSettings
    state_file_path: str = 'storage.json'
    sync_interval: float = 30
    batch_size: int = 100


config = Config.parse_file('config.json')
