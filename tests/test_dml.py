from db.base_test import PostgreSQLTestCase
from db.helpers import DbActions


class TestUserDML(PostgreSQLTestCase):
    TEST_TABLE_NAME = 'People'

    def setUp(self):
        super().setUp()
        self.db = DbActions(self._cursor, self.TEST_TABLE_NAME)

    def test_01_0_insert_and_verify_properties(self):
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

    def test_01_1_insert_full_record_success(self):
        """№ 1-1: Проверка INSERT (успешная вставка полной записи в таблицу 'People')"""
        user_data = {
            "FirstName": "Ivan",
            "LastName": "Ivanov",
            "DataOfBirth": "1990-01-01"
        }

        self.db.insert_record(user_data)
        self.assertEqual(self._cursor.rowcount, 1, "Команда INSERT должна вернуть 1 измененную строку")

        sql = \
            f'SELECT * FROM "{self.TEST_TABLE_NAME}" WHERE "FirstName" = %s AND "LastName" = %s AND "DataOfBirth" = %s'
        self._cursor.execute(sql, tuple(user_data.values()))
        record = self._cursor.fetchone()

        self.assertIsNotNone(record, "Запись не найдена в таблице")

        self.assertEqual(record[1], user_data["FirstName"])
        self.assertEqual(record[2], user_data["LastName"])
        self.assertEqual(str(record[3]), user_data["DataOfBirth"])

        self.assertIsNotNone(record[0], "Поле Index не должно быть NULL")
        self.assertIsInstance(record[0], int, "Поле Index должно быть числовым (Integer/Serial)")

    def test_01_2_insert_multiple_records(self):
        """№ 1-2 Проверка INSERT (массовая вставка нескольких записей в таблицу 'People')"""
        columns = ["FirstName", "LastName", "DataOfBirth"]
        values = [
            ('Gretta', 'Watson', '1991-02-02'),
            ('Polla', 'Kimbel', '1992-03-03')
        ]
        rows_affected = self.db.insert_many(columns, values)
        self.assertEqual(rows_affected, 2, "Должно быть вставлено ровно 2 записи")
        sql_count = f"""
            SELECT COUNT(*) FROM "{self.TEST_TABLE_NAME}" 
            WHERE ("FirstName", "LastName", "DataOfBirth") IN (%s, %s)
        """
        count_res = self.execute_query(sql_count, (values[0], values[1]))
        self.assertEqual(count_res[0], 2, "Запрос должен найти 2 вставленные записи")
        sql_unique = f'SELECT COUNT(DISTINCT "Index") = COUNT(*) FROM "{self.TEST_TABLE_NAME}"'
        unique_res = self.execute_query(sql_unique)
        self.assertTrue(unique_res[0], "Все индексы в таблице должны быть уникальными")
        sql_check_data = f"""
            SELECT "FirstName", "LastName", "DataOfBirth" 
            FROM "{self.TEST_TABLE_NAME}" 
            WHERE "FirstName" = 'Gretta'
        """
        gretta_record = self.execute_query(sql_check_data)
        self.assertEqual(gretta_record[0], 'Gretta')
        self.assertEqual(str(gretta_record[2]), '1991-02-02')

    def test_01_3_insert_min_fields(self):
        """№ 1-3 Проверка INSERT (вставка записи с минимально необходимым набором полей)"""
        user_data = {
            "FirstName": "Masha",
            "LastName": "Sidorova"
        }
        rows_affected = self.db.insert_record(user_data)
        self.assertEqual(rows_affected, 1, "Команда INSERT должна вернуть 1 строку")
        sql_select = f'SELECT * FROM "{self.TEST_TABLE_NAME}" WHERE "FirstName" = %s AND "LastName" = %s'
        record = self.execute_query(sql_select, (user_data["FirstName"], user_data["LastName"]))

        self.assertIsNotNone(record, "Запись не найдена в таблице")
        self.assertEqual(record[1], user_data["FirstName"])
        self.assertEqual(record[2], user_data["LastName"])
        self.assertIsNone(record[3], "Поле DataOfBirth должно содержать NULL")
        sql_null_check = f'SELECT ("DataOfBirth" IS NULL) FROM "{self.TEST_TABLE_NAME}" WHERE "FirstName" = %s'
        null_res = self.execute_query(sql_null_check, (user_data["FirstName"],))
        self.assertTrue(null_res[0], "Запрос IS NULL должен вернуть True (t)")
        self.assertIsNotNone(record[0], "Поле Index не должно быть NULL")
        self.assertIsInstance(record[0], int, "Поле Index должно быть числовым (Serial)")
        sql_index_check = f'SELECT ("Index" IS NOT NULL) FROM "{self.TEST_TABLE_NAME}" WHERE "FirstName" = %s'
        index_res = self.execute_query(sql_index_check, (user_data["FirstName"],))
        self.assertTrue(index_res[0], "Запрос IS NOT NULL должен вернуть True (t)")

    def test_01_4_select_all_records(self):
        """№ 1-4: Проверка SELECT * (возврат всех записей таблицы 'People')"""
        columns = ["FirstName", "LastName", "DataOfBirth"]
        values = [
            ('Ivan', 'Ivanov', '1990-01-01'),
            ('Petr', 'Petrov', '1985-05-12'),
            ('Anna', 'Sidorova', '1995-11-20')
        ]
        rows_inserted = self.db.insert_many(columns, values)
        self.assertEqual(rows_inserted, 3, "Должно быть вставлено ровно 3 записи")
        self._cursor.execute(f'SELECT * FROM "{self.TEST_TABLE_NAME}" ORDER BY "Index" ASC')
        records = self._cursor.fetchall()
        self.assertEqual(len(records), 3, f"Ожидалось 3 строки, но получено {len(records)}")
        for record in records:
            self.assertEqual(len(record), 4, "Каждая строка должна содержать 4 колонки (Index, FN, LN, DOB)")
        for i, val in enumerate(values):
            self.assertEqual(records[i][1], val[0], f"Имя в строке {i} не совпадает")
            self.assertEqual(records[i][2], val[1], f"Фамилия в строке {i} не совпадает")
            self.assertEqual(str(records[i][3]), val[2], f"Дата рождения в строке {i} не совпадает")
