import os
from dataclasses import dataclass
from dotenv import load_dotenv
load_dotenv()


@dataclass
class DBConfig:
    """Класс для хранения конфигурации подключения к PostgreSQL"""

    host: str
    database: str
    user: str
    password: str
    port: int

    @classmethod
    def from_env(cls):
        """Фабричный метод для создания конфига из .env"""

        try:
            return cls(
                host=os.getenv('DB_HOST', 'localhost'),
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                port=int(os.getenv('DB_PORT', '5432'))
            )
        except (TypeError, ValueError) as e:
            raise ValueError(f"Ошибка в конфигурации .env: {e}")
