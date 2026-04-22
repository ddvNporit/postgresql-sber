import os
import sys
import unittest
import psycopg2
from dotenv import load_dotenv
from db.config import DBConfig


class PostgreSQLTestCase(unittest.TestCase):
    _connection = None
    _cursor = None
    _config = None
    TEST_TABLE_NAME = None

    @classmethod
    def setUpClass(cls):
        """Установка соединения один раз на весь класс тестов"""

        cls._config = cls._load_config()
        cls.TEST_TABLE_NAME = cls._config.default_table

        try:
            cls._connection = psycopg2.connect(
                host=cls._config.host,
                database=cls._config.database,
                user=cls._config.user,
                password=cls._config.password,
                port=cls._config.port
            )
            cls._cursor = cls._connection.cursor()
            cls._verify_table_exists()
        except Exception as e:
            print(f"Ошибка подключения или инициализации: {e}")
            sys.exit(1)

    @classmethod
    def _load_config(cls) -> DBConfig:
        """Логика загрузки конфигурации с поддержкой кастомного пути через env="""

        env_path = None
        for arg in sys.argv:
            if arg.startswith("env="):
                env_path = arg.split("=")[1]
                break
        if env_path:
            if os.path.exists(env_path):
                print(f"Загрузка конфигурации из файла: {env_path}")
                load_dotenv(dotenv_path=env_path, override=True)
            else:
                print(f"Ошибка: Файл конфигурации {env_path} не найден.")
                sys.exit(1)
        else:
            load_dotenv()

        try:
            return DBConfig.from_env()
        except Exception as e:
            print(f"Config error: {e}")
            sys.exit(1)

    @classmethod
    def tearDownClass(cls):
        """Закрытие соединений"""

        if cls._cursor:
            cls._cursor.close()
        if cls._connection:
            cls._connection.close()

    def setUp(self):
        """Подготовка перед каждым тестом: изоляция и очистка"""

        self._connection.rollback()
        self._connection.autocommit = False
        self._cursor.execute(f'TRUNCATE TABLE "{self.TEST_TABLE_NAME}" RESTART IDENTITY CASCADE')

    def tearDown(self):
        """Откат транзакции после теста"""

        if self._connection and not self._connection.closed:
            self._connection.rollback()

    @classmethod
    def _verify_table_exists(cls):
        """Проверка структуры перед началом тестов"""

        query = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s);"
        cls._cursor.execute(query, (cls.TEST_TABLE_NAME,))
        if not cls._cursor.fetchone()[0]:
            print(f"Table Error: Таблица '{cls.TEST_TABLE_NAME}' не найдена в БД '{cls._config.database}'.")
            sys.exit(1)

    def execute_query(self, query: str, params: tuple = None):
        """Выполнение запроса и возврат одной строки (или None)"""

        self._cursor.execute(query, params)
        try:
            return self._cursor.fetchone()
        except psycopg2.ProgrammingError:
            return None
