import os
from dataclasses import dataclass

@dataclass
class DBConfig:
    """Класс для хранения конфигурации подключения к PostgreSQL"""

    host: str
    database: str
    user: str
    password: str
    port: int
    default_table: str

    @classmethod
    def from_env(cls):
        try:
            return cls(
                host=os.getenv('DB_HOST', 'localhost'),
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD', ''),
                port=int(os.getenv('DB_PORT', '5432')),
                default_table=os.getenv('DB_DEFAULT_TABLE', 'People')
            )
        except (TypeError, ValueError) as e:
            raise ValueError(f"Ошибка парсинга конфигурации: {e}")