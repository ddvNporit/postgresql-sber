import psycopg2
from psycopg2 import errors
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
            (self.assertEqual
             (len(record), 4, "Каждая строка должна содержать 4 колонки (Index, FN, LN, DOB)"))
        for i, val in enumerate(values):
            self.assertEqual(records[i][1], val[0], f"Имя в строке {i} не совпадает")
            self.assertEqual(records[i][2], val[1], f"Фамилия в строке {i} не совпадает")
            self.assertEqual(str(records[i][3]), val[2], f"Дата рождения в строке {i} не совпадает")

    def test_01_5_first_name_boundaries(self):
        """№ 1-5 Проверка граничных значений для столбца 'FirstName' (varchar 255)"""

        data_min = {"FirstName": "A", "LastName": "Ivanov", "DataOfBirth": "1990-01-01"}
        res_min = self.db.insert_record(data_min)
        self.assertEqual(res_min, 1, "Не удалось вставить строку с FirstName из 1 символа")
        long_name = "B" * 255
        data_max = {"FirstName": long_name, "LastName": "Petrov", "DataOfBirth": "1990-01-01"}
        res_max = self.db.insert_record(data_max)
        self.assertEqual(res_max, 1, "Не удалось вставить строку с FirstName из 255 символов")
        data_empty = {"FirstName": "", "LastName": "Kuznetsov", "DataOfBirth": "1990-01-01"}
        res_empty = self.db.insert_record(data_empty)
        self.assertEqual(res_empty, 1, "Не удалось вставить строку с пустым FirstName")
        self._cursor.execute(f'SELECT COUNT(*) FROM "{self.TEST_TABLE_NAME}"')
        count = self._cursor.fetchone()[0]
        self.assertEqual(count, 3, f"Ожидалось 3 строки в таблице, но получено {count}")
        too_long_name = "C" * 256
        data_error = {"FirstName": too_long_name, "LastName": "Sidorov", "DataOfBirth": "1990-01-01"}
        with self.assertRaises(psycopg2.DataError):
            self.db.insert_record(data_error)

    def test_01_6_manual_index_insert(self):
        """№ 1-6 Проверка столбца 'Index' (вставка с ручным указанием 'Index' = 1)"""
        manual_data = {
            "Index": 1,
            "FirstName": "Petr",
            "LastName": "Petrov",
            "DataOfBirth": "1995-05-15"
        }
        rows_affected = self.db.insert_record(manual_data)
        self.assertEqual(rows_affected, 1, "Команда INSERT с ручным Index должна вернуть 1 строку")
        sql = f'SELECT * FROM "{self.TEST_TABLE_NAME}" WHERE "Index" = 1'
        record = self.execute_query(sql)
        self.assertIsNotNone(record, "Запись с Index=1 не найдена")
        self.assertEqual(record[0], 1, "Значение в колонке Index должно быть строго равно 1")
        self.assertEqual(record[1], "Petr")

    def test_01_7_index_type_conversion(self):
        """№ 1-7 Проверка столба 'Index' (вставка с преобразованием типов 2.00)"""

        data = {
            "Index": 2.00,
            "FirstName": "Sidor",
            "LastName": "Sidorov",
            "DataOfBirth": "1988-10-10"
        }
        rows_affected = self.db.insert_record(data)
        self.assertEqual(rows_affected, 1, "БД должна автоматически привести 2.00 к целому числу")
        sql = f'SELECT "Index" FROM "{self.TEST_TABLE_NAME}" WHERE "FirstName" = %s'
        record = self.execute_query(sql, (data["FirstName"],))
        self.assertIsNotNone(record, "Запись не найдена")
        self.assertEqual(record[0], 2, "Значение Index в базе должно быть равно 2")
        self.assertIsInstance(record[0], int, "Тип данных в базе должен остаться целочисленным")

    def test_01_8_index_check_constraint_violation(self):
        """№ 1-8 Проверка столба 'Index' (вставка с ручным указанием 'Index' = 0)"""

        invalid_data = {
            "Index": 0,
            "FirstName": "Anna",
            "LastName": "Smirnova",
            "DataOfBirth": "2000-12-31"
        }
        with self.assertRaises(psycopg2.errors.CheckViolation) as cm:
            self.db.insert_record(invalid_data)
        self.assertIn('violates check constraint', str(cm.exception).lower())

    def test_01_9_index_out_of_range(self):
        """№ 1-9 Проверка столба 'Index' (вставка значения 2147483648 - выход за границы)"""

        invalid_data = {
            "Index": 2147483648,
            "FirstName": "Anna",
            "LastName": "Smirnova",
            "DataOfBirth": "2000-12-31"
        }

        with self.assertRaises(psycopg2.DatabaseError) as cm:
            self.db.insert_record(invalid_data)

        sqlstate = cm.exception.pgcode
        self.assertIn(sqlstate, ['22003', '23514'],
                      f"Ожидался код ошибки 22003 или 23514, но получен {sqlstate}")

        error_msg = str(cm.exception).lower()
        self.assertTrue('range' in error_msg or 'violate' in error_msg,
                        f"Текст ошибки не соответствует ожидаемому: {error_msg}")

    def test_01_10_index_maximum_valid_value(self):
        """№ 1-10 Проверка столба 'Index' (вставка значения 2147483647 - максимум)"""

        max_valid_index = 2147483647
        data = {
            "Index": max_valid_index,
            "FirstName": "Anna",
            "LastName": "Smirnova",
            "DataOfBirth": "2000-12-31"
        }
        rows_affected = self.db.insert_record(data)
        self.assertEqual(rows_affected, 1, "Запись с максимальным Index должна быть вставлена успешно")
        sql = f'SELECT "Index", "FirstName" FROM "{self.TEST_TABLE_NAME}" WHERE "Index" = %s'
        record = self.execute_query(sql, (max_valid_index,))
        self.assertIsNotNone(record, "Запись не найдена")
        self.assertEqual(record[0], max_valid_index, "Значение Index в базе должно быть 2147483647")
        self.assertEqual(record[1], "Anna")

    def test_01_11_index_negative_value(self):
        """№ 1-11 Проверка столба 'Index' (вставка с ручным указанием 'Index' < 0)"""

        invalid_data = {
            "Index": -1,
            "FirstName": "Petr",
            "LastName": "Petrov",
            "DataOfBirth": "1995-05-15"
        }
        with self.assertRaises(psycopg2.IntegrityError) as cm:
            self.db.insert_record(invalid_data)
        sqlstate = cm.exception.pgcode
        self.assertEqual(sqlstate, '23514', f"Ожидался код ошибки 23514, но получен {sqlstate}")
        self.assertIn('violates check constraint', str(cm.exception).lower())

    def test_01_12_index_string_value(self):
        """№ 1-12 Проверка столба 'Index' (вставка строкового значения в Integer)"""

        invalid_data = {
            "Index": "Petr",
            "FirstName": "Petr",
            "LastName": "Petrov",
            "DataOfBirth": "1995-05-15"
        }

        with self.assertRaises(psycopg2.DatabaseError) as cm:
            self.db.insert_record(invalid_data)
        sqlstate = cm.exception.pgcode
        self.assertEqual(sqlstate, '22P02', f"Ожидался код ошибки 22P02, но получен {sqlstate}")
        error_msg = str(cm.exception).lower()
        self.assertIn('invalid input syntax for type integer', error_msg)

    def test_01_13_dob_integer_instead_of_date(self):
        """№ 1-13 Проверка столба 'DataOfBirth' (вставка числа вместо даты)"""

        invalid_data = {
            "FirstName": "Ivan",
            "LastName": "Ivanov",
            "DataOfBirth": 1
        }
        with self.assertRaises(psycopg2.DatabaseError) as cm:
            self.db.insert_record(invalid_data)

        sqlstate = cm.exception.pgcode
        self.assertEqual(sqlstate, '42804', f"Ожидался код ошибки 42804, но получен {sqlstate}")
        self.assertIn('is of type date but expression is of type integer', str(cm.exception).lower())

    def test_01_14_dob_valid_string_formats(self):
        """№ 1-14 Проверка столба 'DataOfBirth' (вставка валидной даты в строковом формате)"""

        data_1 = {"Index": 1, "FirstName": "Ivan", "LastName": "Ivanov", "DataOfBirth": "01-12-1990"}
        res1 = self.db.insert_record(data_1)
        self.assertEqual(res1, 1)
        data_2 = {"Index": 2, "FirstName": "Ivan", "LastName": "Ivanov", "DataOfBirth": "1975-January-1"}
        res2 = self.db.insert_record(data_2)
        self.assertEqual(res2, 1)
        self._cursor.execute(
            f'SELECT "Index", "DataOfBirth" FROM "{self.TEST_TABLE_NAME}" WHERE "Index" IN (1, 2) ORDER BY "Index"')
        records = self._cursor.fetchall()
        self.assertEqual(str(records[0][1]), '1990-12-01')
        self.assertEqual(str(records[1][1]), '1975-01-01')

    def test_01_15_dob_invalid_month(self):
        """№ 1-15 Проверка невалидного значения месяца в столбе 'DataOfBirth' (13 месяц)"""

        invalid_data = {
            "Index": 2,
            "FirstName": "Error",
            "LastName": "User",
            "DataOfBirth": "1975-13-2"
        }
        with self.assertRaises(psycopg2.DatabaseError) as cm:
            self.db.insert_record(invalid_data)

        sqlstate = cm.exception.pgcode
        self.assertEqual(sqlstate, '22008', f"Ожидался код ошибки 22008, но получен {sqlstate}")
        self.assertIn('date/time field value out of range', str(cm.exception).lower())