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
    COLUMNS = {'INDEX': None, 'FNAME': None, 'LNAME': None, 'DOB': None}
    table_schema = []

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
            cls._fetch_table_metadata()

        except Exception as e:
            print(f"Ошибка подключения или инициализации: {e}")
            sys.exit(1)

    @classmethod
    def _load_config(cls) -> DBConfig:
        if os.getenv("DB_NAME"):
            return DBConfig.from_env()
        env_path = os.getenv("ENV_FILE")
        if not env_path:
            for i, arg in enumerate(sys.argv):
                if arg == "-env" and i + 1 < len(sys.argv):
                    env_path = sys.argv[i + 1].strip("'").strip('"')
                    break
        if env_path:
            if os.path.exists(env_path):
                print(f"--- Используется конфигурация: {env_path} ---")
                load_dotenv(dotenv_path=env_path, override=True)
            else:
                print(f"Ошибка: Указанный файл конфигурации '{env_path}' не найден.")
                sys.exit(1)
        else:
            if os.path.exists('.env'):
                print("--- Используется конфигурация по умолчанию .env ---")
                load_dotenv(override=True)
            else:
                print("Ошибка: Нет конфигурации (.env не найден и ENV_FILE/ -env не указаны)")
                sys.exit(1)

        try:
            return DBConfig.from_env()
        except Exception as e:
            print(f"Ошибка парсинга конфигурации: {e}")
            sys.exit(1)

    @classmethod
    def tearDownClass(cls):
        if cls._cursor:
            cls._cursor.close()
        if cls._connection:
            cls._connection.close()

    def setUp(self):
        """Очистка таблицы перед каждым тестом"""

        self._connection.rollback()
        self._cursor.execute(f'TRUNCATE TABLE "{self.TEST_TABLE_NAME}" RESTART IDENTITY CASCADE')
        self._connection.commit()

    def tearDown(self):
        if self._connection and not self._connection.closed:
            self._connection.rollback()

    @classmethod
    def _verify_table_exists(cls):
        """Проверка существования таблицы с учетом регистра"""

        query = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s OR table_name = LOWER(%s));"
        cls._cursor.execute(query, (cls.TEST_TABLE_NAME, cls.TEST_TABLE_NAME))
        if not cls._cursor.fetchone()[0]:
            print(f"Table Error: Таблица '{cls.TEST_TABLE_NAME}' не найдена.")
            sys.exit(1)

    @classmethod
    def _fetch_table_metadata(cls):
        """Универсальное получение структуры и ролей"""

        query = """
            SELECT column_name, data_type, is_nullable, character_maximum_length, column_default
            FROM information_schema.columns 
            WHERE table_name = %s OR table_name = LOWER(%s)
            ORDER BY ordinal_position;
        """
        cls._cursor.execute(query, (cls.TEST_TABLE_NAME, cls.TEST_TABLE_NAME))
        rows = cls._cursor.fetchall()

        if not rows:
            print(f"Metadata Error: Столбцы для '{cls.TEST_TABLE_NAME}' не найдены.")
            return
        cls.table_schema = [
            {
                'name': row[0],
                'type': row[1],
                'nullable': row[2] == 'YES',
                'max_len': row[3],
                'has_default': row[4] is not None
            } for row in rows
        ]

        cls.COLUMNS = {'INDEX': None, 'FNAME': None, 'LNAME': None, 'DOB': None}
        for col in cls.table_schema:
            name, dtype = col['name'], col['type'].lower()
            if ('int' in dtype or 'serial' in dtype) and not cls.COLUMNS['INDEX']:
                cls.COLUMNS['INDEX'] = name
            elif 'date' in dtype and not cls.COLUMNS['DOB']:
                cls.COLUMNS['DOB'] = name
            elif 'char' in dtype or 'text' in dtype:
                if not cls.COLUMNS['FNAME']:
                    cls.COLUMNS['FNAME'] = name
                elif not cls.COLUMNS['LNAME']:
                    cls.COLUMNS['LNAME'] = name

    @staticmethod
    def generate_test_value(col: dict):
        """Генерирует валидное значение для столбца"""
        t = col['type'].lower()
        if 'int' in t or 'serial' in t:
            return 100
        if 'char' in t or 'text' in t:
            length = col['max_len'] or 10
            return "TestData"[:length]
        if 'date' in t:
            return "2000-01-01"
        if 'bool' in t:
            return True
        return None

    def execute_query(self, query: str, params: tuple = None):
        self._cursor.execute(query, params)
        try:
            return self._cursor.fetchone()
        except psycopg2.ProgrammingError:
            return None

    def assertSqlError(self, expected_codes, func, *args, **kwargs):
        if isinstance(expected_codes, str):
            expected_codes = [expected_codes]
        with self.assertRaises(psycopg2.Error) as cm:
            func(*args, **kwargs)
        self.assertIn(cm.exception.pgcode, expected_codes)
