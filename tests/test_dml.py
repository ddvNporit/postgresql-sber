from db.base_test import PostgreSQLTestCase
from db.helpers import DbActions


class TestUserDML(PostgreSQLTestCase):
    TEST_TABLE_NAME = 'People'

    def setUp(self):
        super().setUp()
        self.db = DbActions(self._cursor, self.TEST_TABLE_NAME)

    def test_insert_and_verify_properties(self):
        """№ 1-0 Проверка свойств таблицы 'People'"""
        user_data = {
            "FirstName": "Ivan1",
            "LastName": "Ivanov1",
            "DataOfBirth": "1989-05-15"
        }

        self.db.insert_record(user_data)

        cols_to_check = ["Index", "FirstName", "LastName", "DataOfBirth"]
        res = self.db.get_column_types(cols_to_check, user_data)

        self.assertIsNotNone(res)
        self.assertRegex(res[0], r'^\d+: integer$')
        self.assertEqual(res[1], "Ivan1: character varying")
        self.assertEqual(res[2], "Ivanov1: character varying")
        self.assertEqual(res[3], "1989-05-15: date")

        year_res = self.db.get_extracted_part("YEAR", "DataOfBirth", "FirstName", "Ivan1")
        self.assertEqual(int(year_res[0]), 1989)

    def test_insert_full_record_success(self):
        """№ 1-1: Проверка INSERT (успешная вставка полной записи в таблицу 'People')"""
        user_data = {
            "FirstName": "Ivan",
            "LastName": "Ivanov",
            "DataOfBirth": "1990-01-01"
        }

        self.db.insert_record(user_data)
        self.assertEqual(self._cursor.rowcount, 1, "Команда INSERT должна вернуть 1 измененную строку")

        sql = f'SELECT * FROM "{self.TEST_TABLE_NAME}" WHERE "FirstName" = %s AND "LastName" = %s AND "DataOfBirth" = %s'
        self._cursor.execute(sql, tuple(user_data.values()))
        record = self._cursor.fetchone()

        self.assertIsNotNone(record, "Запись не найдена в таблице")

        self.assertEqual(record[1], user_data["FirstName"])
        self.assertEqual(record[2], user_data["LastName"])
        self.assertEqual(str(record[3]), user_data["DataOfBirth"])

        self.assertIsNotNone(record[0], "Поле Index не должно быть NULL")
        self.assertIsInstance(record[0], int, "Поле Index должно быть числовым (Integer/Serial)")