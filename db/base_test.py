import os
import sys
import unittest
from dataclasses import dataclass

import psycopg2
from dotenv import load_dotenv

load_dotenv()


@dataclass
class DBConfig:
    """Класс для хранения конфигурации подключения"""
    host: str
    database: str
    user: str
    password: str
    port: int


class PostgreSQLTestCase(unittest.TestCase):
    """
    Базовый класс для тестирования DML операций в PostgreSQL.
    """
    _config = None
    _connection = None
    _cursor = None

    TEST_TABLE_NAME = "People"

    @classmethod
    def setUpClass(cls):
        """Выполняется один раз перед запуском всех тестов в классе"""
        cls._config = cls._load_config()
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
        except psycopg2.OperationalError as e:
            print(f"Connection error: {e}")
            sys.exit(1)

    @classmethod
    def tearDownClass(cls):
        """Закрытие соединения после всех тестов"""
        if cls._cursor:
            cls._cursor.close()
        if cls._connection:
            cls._connection.close()

    def setUp(self):
        """Подготовка перед каждым тестом"""
        self._connection.rollback()

        self._connection.autocommit = False

        self._cursor.execute(f'TRUNCATE TABLE "{self.TEST_TABLE_NAME}" RESTART IDENTITY CASCADE')

    def tearDown(self):
        """
        Завершение после каждого теста:
        Откатываем транзакцию.
        """
        if not self._connection.closed:
            self._connection.rollback()

    @classmethod
    def _load_config(cls) -> DBConfig:
        """Загрузка настроек из .env"""
        try:
            return DBConfig(
                host=os.getenv('DB_HOST', 'localhost'),
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                port=int(os.getenv('DB_PORT', '5432'))
            )
        except (TypeError, ValueError) as e:
            print(f"Config error: Проверьте переменные окружения в .env ({e})")
            sys.exit(1)

    @classmethod
    def _verify_table_exists(cls):
        """Проверка наличия таблицы перед стартом тестов"""
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            );
        """
        cls._cursor.execute(query, (cls.TEST_TABLE_NAME,))
        if not cls._cursor.fetchone()[0]:
            print(f"Table Error: Таблица '{cls.TEST_TABLE_NAME}' не найдена в БД.")
            sys.exit(1)

    def execute_query(self, query: str, params: tuple = None):
        """
        Универсальный метод для выполнения запроса и получения одной строки.
        """
        self._cursor.execute(query, params)
        try:
            return self._cursor.fetchone()
        except psycopg2.ProgrammingError:
            return None

    def print_report(self, info_message: str):
        """Метод для вывода отчета в консоль (опционально)"""
        print(f"\n[REPORT] Test: {self._testMethodName}")
        print(f"Target Table: {self.TEST_TABLE_NAME}")
        print(f"Message: {info_message}")
        print("-" * 30)
