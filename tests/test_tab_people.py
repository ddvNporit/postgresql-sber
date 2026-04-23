"""
A testing framework for checking PostgreSQL DML operations
author: Dmitry Denisov
GitHub: https://github.com/ddvNporit/postgresql-sber
Email: dima.nporit@gmail.com
"""
import unittest

import psycopg2

from db.base_test import PostgreSQLTestCase
from db.helpers import DbActions


class TestPeopleDML(PostgreSQLTestCase):
    db: DbActions = None

    @property
    def COL_INDEX(self):
        return self.COLUMNS['INDEX']

    @property
    def COL_FIRST_NAME(self):
        return self.COLUMNS['FNAME']

    @property
    def COL_LAST_NAME(self):
        return self.COLUMNS['LNAME']

    @property
    def COL_DOB(self):
        return self.COLUMNS['DOB']

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.db = DbActions(cls._cursor, cls.TEST_TABLE_NAME)

        required_roles = ['INDEX', 'FNAME', 'LNAME', 'DOB']
        missing = [r for r in required_roles if cls.COLUMNS.get(r) is None]

        if missing or cls.TEST_TABLE_NAME != "People":
            raise unittest.SkipTest(
                f"Класс TestPeopleDML пропущен: таблица '{cls.TEST_TABLE_NAME}' "
                f"не соответствует структуре People"
            )

    def setUp(self):
        super().setUp()
        self.db.cursor = self._cursor

    def test_1_00_insert_and_verify_properties(self):
        """№ 1-0 Проверка свойств таблицы 'People'"""

        user_data = {
            self.COL_FIRST_NAME: "Ivan1",
            self.COL_LAST_NAME: "Ivanov1",
            self.COL_DOB: "1989-05-15"
        }
        self.db.insert_record(user_data)
        cols_to_check = [self.COL_INDEX, self.COL_FIRST_NAME, self.COL_LAST_NAME, self.COL_DOB]
        res = self.db.get_column_types(cols_to_check, user_data)

        self.assertIsNotNone(res)
        self.assertRegex(res[0], r'^\d+: integer$')
        self.assertEqual(res[1], f"Ivan1: character varying")
        self.assertEqual(res[2], f"Ivanov1: character varying")
        self.assertEqual(res[3], "1989-05-15: date")

        year_res = (self.db.get_extracted_part
                    ("YEAR", self.COL_DOB, self.COL_FIRST_NAME, "Ivan1"))
        self.assertEqual(int(year_res[0]), 1989)

    def test_1_01_insert_full_record_success(self):
        """№ 1-1: Проверка INSERT (успешная вставка полной записи в таблицу 'People')"""

        user_data = {
            self.COL_FIRST_NAME: "Ivan",
            self.COL_LAST_NAME: "Ivanov",
            self.COL_DOB: "1990-01-01"
        }
        self.db.insert_record(user_data)
        self.assertEqual(self._cursor.rowcount, 1, "Команда INSERT должна вернуть 1 измененную строку")
        sql = \
            (f'SELECT * FROM "{self.TEST_TABLE_NAME}" WHERE "{self.COL_FIRST_NAME}" = %s AND "'
             f'{self.COL_LAST_NAME}" = %s AND "{self.COL_DOB}" = %s')
        self._cursor.execute(sql, tuple(user_data.values()))
        record = self._cursor.fetchone()
        self.assertIsNotNone(record, "Запись не найдена в таблице")
        self.assertEqual(record[1], user_data[self.COL_FIRST_NAME])
        self.assertEqual(record[2], user_data[self.COL_LAST_NAME])
        self.assertEqual(str(record[3]), user_data[self.COL_DOB])
        self.assertIsNotNone(record[0], f"Поле {self.COL_INDEX} не должно быть NULL")
        self.assertIsInstance(record[0], int,
                              f"Поле {self.COL_INDEX} должно быть числовым (Integer/Serial)")

    def test_1_02_insert_multiple_records(self):
        """№ 1-2 Проверка INSERT (массовая вставка нескольких записей в таблицу 'People')"""

        columns = [self.COL_FIRST_NAME, self.COL_LAST_NAME, self.COL_DOB]
        values = [
            ('Gretta', 'Watson', '1991-02-02'),
            ('Polla', 'Kimbel', '1992-03-03')
        ]
        rows_affected = self.db.insert_many(columns, values)
        self.assertEqual(rows_affected, 2, "Должно быть вставлено ровно 2 записи")
        sql_count = f"""
            SELECT COUNT(*) FROM "{self.TEST_TABLE_NAME}" 
            WHERE ("{self.COL_FIRST_NAME}", "{self.COL_LAST_NAME}", "{self.COL_DOB}") IN (%s, %s)
        """
        count_res = self.execute_query(sql_count, (values[0], values[1]))
        self.assertEqual(count_res[0], 2, "Запрос должен найти 2 вставленные записи")
        sql_unique = f'SELECT COUNT(DISTINCT "{self.COL_INDEX}") = COUNT(*) FROM "{self.TEST_TABLE_NAME}"'
        unique_res = self.execute_query(sql_unique)
        self.assertTrue(unique_res[0], f"Все индексы в таблице {self.COL_INDEX} должны быть уникальными")
        sql_check_data = f"""
            SELECT "{self.COL_FIRST_NAME}", "{self.COL_LAST_NAME}", "{self.COL_DOB}" 
            FROM "{self.TEST_TABLE_NAME}" 
            WHERE "{self.COL_FIRST_NAME}" = 'Gretta'
        """
        gretta_record = self.execute_query(sql_check_data)
        self.assertEqual(gretta_record[0], 'Gretta')
        self.assertEqual(str(gretta_record[2]), '1991-02-02')

    def test_1_03_insert_min_fields(self):
        """№ 1-3 Проверка INSERT (вставка записи с минимально необходимым набором полей)"""

        user_data = {
            self.COL_FIRST_NAME: "Masha",
            self.COL_LAST_NAME: "Sidorova"
        }
        rows_affected = self.db.insert_record(user_data)
        self.assertEqual(rows_affected, 1, "Команда INSERT должна вернуть 1 строку")
        sql_select = \
            f'SELECT * FROM "{self.TEST_TABLE_NAME}" WHERE "{self.COL_FIRST_NAME}" = %s AND "{self.COL_LAST_NAME}" = %s'
        record = (self.execute_query
                  (sql_select, (user_data[self.COL_FIRST_NAME], user_data[self.COL_LAST_NAME])))
        self.assertIsNotNone(record, "Запись не найдена в таблице")
        self.assertEqual(record[1], user_data[self.COL_FIRST_NAME])
        self.assertEqual(record[2], user_data[self.COL_LAST_NAME])
        self.assertIsNone(record[3], f"Поле {self.COL_DOB} должно содержать NULL")
        sql_null_check = \
            f'SELECT ("{self.COL_DOB}" IS NULL) FROM "{self.TEST_TABLE_NAME}" WHERE "{self.COL_FIRST_NAME}" = %s'
        null_res = self.execute_query(sql_null_check, (user_data[self.COL_FIRST_NAME],))
        self.assertTrue(null_res[0], "Запрос IS NULL должен вернуть True (t)")
        self.assertIsNotNone(record[0], f"Поле {self.COL_INDEX} не должно быть NULL")
        self.assertIsInstance(record[0], int, f"Поле {self.COL_INDEX} должно быть числовым (Serial)")

    def test_1_04_select_all_records(self):
        """№ 1-4: Проверка SELECT * (возврат всех записей таблицы 'People')"""

        columns, values = self.get_test_data()
        rows_inserted = self.db.insert_many(columns, values)
        self.assertEqual(rows_inserted, 3, "Должно быть вставлено ровно 3 записи")
        self._cursor.execute(f'SELECT * FROM "{self.TEST_TABLE_NAME}" ORDER BY "{self.COL_INDEX}" ASC')
        records = self._cursor.fetchall()
        self.assertEqual(len(records), 3, f"Ожидалось 3 строки, но получено {len(records)}")
        for record in records:
            (self.assertEqual
             (len(record), 4,
              f"Строка должна содержать 4 колонки "
              f"({self.COL_INDEX}, {self.COL_FIRST_NAME}, {self.COL_LAST_NAME}, {self.COL_DOB})"))
        for i, val in enumerate(values):
            self.assertEqual(records[i][1], val[0], f"Имя в строке {i} не совпадает")
            self.assertEqual(records[i][2], val[1], f"Фамилия в строке {i} не совпадает")
            self.assertEqual(str(records[i][3]), val[2], f"Дата рождения в строке {i} не совпадает")

    def test_1_05_first_name_boundaries(self):
        """№ 1-5 Проверка граничных значений для столбца 'FirstName' (varchar 255)"""

        data_min = self.get_test_data(custom_fname="A", as_dict=True)
        self.assertEqual(self.db.insert_record(data_min), 1,
                         f"Ошибка вставки {self.COL_FIRST_NAME} из 1 символа")
        data_max = self.get_test_data(custom_fname="B" * 255, as_dict=True)
        self.assertEqual(self.db.insert_record(data_max), 1,
                         f"Ошибка вставки {self.COL_FIRST_NAME} из 255 символов")
        data_empty = self.get_test_data(custom_fname="", as_dict=True)
        self.assertEqual(self.db.insert_record(data_empty), 1,
                         f"Ошибка вставки пустого {self.COL_FIRST_NAME}")
        self._cursor.execute(f'SELECT COUNT(*) FROM "{self.TEST_TABLE_NAME}"')
        self.assertEqual(self._cursor.fetchone()[0], 3)
        data_error = self.get_test_data(custom_fname="C" * 256, as_dict=True)
        with self.assertRaises(psycopg2.DataError):
            self.db.insert_record(data_error)

    def test_1_06_manual_index_insert(self):
        """№ 1-6 Проверка столбца 'Index' (вставка с ручным указанием 'Index' = 1)"""

        manual_data = self.get_test_data(1)
        rows_affected = self.db.insert_record(manual_data)
        self.assertEqual(rows_affected, 1,
                         f"Команда INSERT с ручным {self.COL_INDEX} должна вернуть 1 строку")
        sql = f'SELECT * FROM "{self.TEST_TABLE_NAME}" WHERE "{self.COL_INDEX}" = 1'
        record = self.execute_query(sql)
        self.assertIsNotNone(record, f"Запись с {self.COL_INDEX}=1 не найдена")
        self.assertEqual(record[0], 1,
                         f"Значение в колонке {self.COL_INDEX} должно быть строго равно 1")
        self.assertEqual(record[1], "Petr")

    def test_1_07_index_type_conversion(self):
        """№ 1-7 Проверка столба 'Index' (вставка с преобразованием типов 2.00)"""

        data = self.get_test_data(index=2.00)
        rows_affected = self.db.insert_record(data)
        self.assertEqual(rows_affected, 1, "БД должна автоматически привести 2.00 к целому числу")
        sql = f'SELECT "{self.COL_INDEX}" FROM "{self.TEST_TABLE_NAME}" WHERE "{self.COL_FIRST_NAME}" = %s'
        record = self.execute_query(sql, (data[self.COL_FIRST_NAME],))
        self.assertIsNotNone(record, "Запись не найдена")
        self.assertEqual(record[0], 2, f"Значение {self.COL_INDEX} в базе должно быть равно 2")
        self.assertIsInstance(record[0], int, "Тип данных в базе должен остаться целочисленным")

    def test_1_08n_index_check_constraint_violation(self):
        """№ 1-8n Проверка столба 'Index' (вставка с ручным указанием 'Index' = 0)"""

        invalid_data = self.get_test_data(index=0)
        self.assertSqlError('23514', self.db.insert_record, invalid_data)

    def test_1_09n_index_out_of_range(self):
        """№ 1-9n Проверка столба 'Index' (вставка значения 2147483648 - выход за границы)"""

        invalid_data = self.get_test_data(index=2147483648)
        self.assertSqlError(['22003', '23514'], self.db.insert_record, invalid_data)

    def test_1_10_index_maximum_valid_value(self):
        """№ 1-10 Проверка столба 'Index' (вставка значения 2147483647 - максимум)"""

        max_valid_index = 2147483647
        data = self.get_test_data(index=max_valid_index)
        rows_affected = self.db.insert_record(data)
        self.assertEqual(rows_affected, 1, "Запись с максимальным Index должна быть вставлена успешно")
        sql = (f'SELECT "{self.COL_INDEX}", "{self.COL_FIRST_NAME}" FROM "{self.TEST_TABLE_NAME}"'
               f' WHERE "{self.COL_INDEX}" = %s')
        record = self.execute_query(sql, (max_valid_index,))
        self.assertIsNotNone(record, "Запись не найдена")
        self.assertEqual(record[0], max_valid_index,
                         f"Значение {self.COL_INDEX} в базе должно быть 2147483647")
        self.assertEqual(record[1], "Petr")

    def test_1_11n_index_negative_value(self):
        """№ 1-11n Проверка столба 'Index' (вставка с ручным указанием 'Index' < 0)"""

        invalid_data = self.get_test_data(index=-1)
        self.assertSqlError('23514', self.db.insert_record, invalid_data)

    def test_1_12n_index_string_value(self):
        """№ 1-12n Проверка столба 'Index' (вставка строкового значения в Integer)"""

        invalid_data = self.get_test_data(index="Petr")
        self.assertSqlError('22P02', self.db.insert_record, invalid_data)

    def test_1_13n_dob_integer_instead_of_date(self):
        """№ 1-13n Проверка столба 'DataOfBirth' (вставка числа вместо даты)"""

        invalid_data = {
            self.COL_FIRST_NAME: "Ivan",
            self.COL_LAST_NAME: "Ivanov",
            self.COL_DOB: 1
        }
        self.assertSqlError('42804', self.db.insert_record, invalid_data)

    def test_1_14_dob_valid_string_formats(self):
        """№ 1-14 Проверка столба 'DataOfBirth' (вставка валидной даты в строковом формате)"""

        data_1 = {self.COL_INDEX: 1, self.COL_FIRST_NAME: "Ivan", self.COL_LAST_NAME: "Ivanov",
                  self.COL_DOB: "01-12-1990"}
        res1 = self.db.insert_record(data_1)
        self.assertEqual(res1, 1)
        data_2 = {self.COL_INDEX: 2, self.COL_FIRST_NAME: "Ivan", self.COL_LAST_NAME: "Ivanov",
                  self.COL_DOB: "1975-January-1"}
        res2 = self.db.insert_record(data_2)
        self.assertEqual(res2, 1)
        self._cursor.execute(
            f'SELECT "{self.COL_INDEX}", "{self.COL_DOB}" FROM "{self.TEST_TABLE_NAME}"'
            f' WHERE "{self.COL_INDEX}" IN (1, 2) ORDER BY "{self.COL_INDEX}"')
        records = self._cursor.fetchall()
        self.assertEqual(str(records[0][1]), '1990-12-01')
        self.assertEqual(str(records[1][1]), '1975-01-01')

    def test_1_15n_dob_invalid_month(self):
        """№ 1-15n Проверка невалидного значения месяца в столбе 'DataOfBirth' (13 месяц)"""

        invalid_data = {
            self.COL_INDEX: 2,
            self.COL_FIRST_NAME: "Error",
            self.COL_LAST_NAME: "User",
            self.COL_DOB: "1975-13-2"
        }
        self.assertSqlError('22008', self.db.insert_record, invalid_data)

    def test_1_16_truncate_and_delete_all(self):
        """№ 1-16 Проверка TRUNCATE и DELETE без параметров"""

        columns, values = self.get_test_data()
        rows_inserted = self.db.insert_many(columns, values)
        self.assertEqual(rows_inserted, 3, "Должно быть вставлено 3 записи")
        self._cursor.execute(f'SELECT COUNT(*) FROM "{self.TEST_TABLE_NAME}"')
        self.assertEqual(self._cursor.fetchone()[0], 3, "Таблица должна содержать 3 строки")
        self._cursor.execute(f'DELETE FROM "{self.TEST_TABLE_NAME}"')
        rows_deleted = self._cursor.rowcount
        self.assertEqual(rows_deleted, 3, f"Ожидалось удаление 3 строк, удалено {rows_deleted}")
        self._cursor.execute(f'SELECT * FROM "{self.TEST_TABLE_NAME}"')
        self.assertEqual(len(self._cursor.fetchall()), 0, "Таблица должна быть пустой")

    def test_1_17_delete_where_in(self):
        """№ 1-17 Проверка DELETE с конструкцией WHERE IN"""

        columns, values = self.get_test_data()
        self.db.insert_many(columns, values)
        self._cursor.execute(f'SELECT "{self.COL_INDEX}" FROM "{self.TEST_TABLE_NAME}"')
        indices = [row[0] for row in self._cursor.fetchall()]
        self.assertEqual(len(indices), 3, "Таблица должна содержать 3 строки")
        self._cursor.execute(f'DELETE FROM "{self.TEST_TABLE_NAME}" WHERE "{self.COL_INDEX}" IN %s', (tuple(indices),))
        self.assertEqual(self._cursor.rowcount, 3,
                         f"Должно быть удалено 3 записи по списку {self.COL_INDEX}")
        self._cursor.execute(f'SELECT COUNT(*) FROM "{self.TEST_TABLE_NAME}"')
        self.assertEqual(self._cursor.fetchone()[0], 0, "Таблица должна быть пустой")

    def test_1_18_select_where_like(self):
        """№ 1-18 Проверка SELECT с конструкцией WHERE LIKE"""

        columns, values = self.get_test_data()
        self.db.insert_many(columns, values)
        self._cursor.execute(
            f'SELECT "{self.COL_FIRST_NAME}", "{self.COL_LAST_NAME}",'
            f' "{self.COL_DOB}" FROM "{self.TEST_TABLE_NAME}" WHERE "{self.COL_FIRST_NAME}" LIKE \'I%%\'')
        result = self._cursor.fetchall()
        self.assertEqual(len(result), 1, "Должна быть найдена ровно 1 запись")
        self.assertEqual(result[0][0], 'Ivan')
        self.assertEqual(result[0][1], 'Ivanov')
        self.assertEqual(str(result[0][2]), '1990-01-01')

    def test_1_19_select_is_null(self):
        """№ 1-19 Проверка SELECT с конструкцией WHERE и оператора IS NULL"""

        check_value = ('Ivan', 'Ivanov')
        check_columns = (self.COL_FIRST_NAME, self.COL_LAST_NAME)
        columns = list(check_columns) + [self.COL_DOB]
        values = [
            (check_value[0], check_value[1], None),
            ('Petr', 'Petrov', '1985-05-12'),
            ('Anna', 'Sidorova', '1995-11-20')
        ]
        self.db.insert_many(columns, values)
        query = \
            (f'SELECT "{check_columns[0]}", "{check_columns[1]}" '
             f'FROM "{self.TEST_TABLE_NAME}" WHERE "{self.COL_DOB}" IS NULL')
        self._cursor.execute(query)
        result = self._cursor.fetchall()
        self.assertEqual(len(result), 1, f"Должна быть найдена ровно 1 запись с NULL в {self.COL_DOB}")
        first_name, last_name = result[0]

        self.assertEqual(first_name, check_value[0],
                         f"Столбец {check_columns[0]} должен быть {check_value[0]}")
        self.assertEqual(last_name, check_value[1],
                         f"Столбец {check_columns[1]} должен быть {check_value[1]}")

    def test_1_20_select_empty_result(self):
        """№ 1-20 Проверка SELECT, возвращающей пустой ответ"""

        columns, values = self.get_test_data(with_null_dob=True)
        self.db.insert_many(columns, values)
        self._cursor.execute(
            f'SELECT "{self.COL_LAST_NAME}" FROM "{self.TEST_TABLE_NAME}" WHERE "{self.COL_FIRST_NAME}" = %s',
            ('Anna1',))
        result = self._cursor.fetchall()
        self.assertEqual(len(result), 0,
                         "Запрос должен вернуть пустой результат для несуществующего имени")

    def test_1_21_update_with_not_and_logic(self):
        """№ 1-21 Проверка UPDATE с конструкцией WHERE и операторов NOT, AND и ="""

        columns, values = self.get_test_data()
        self.db.insert_many(columns, values)
        query = f"""
            UPDATE "{self.TEST_TABLE_NAME}" 
            SET "{self.COL_LAST_NAME}" = %s 
            WHERE NOT "{self.COL_FIRST_NAME}" = %s AND NOT "{self.COL_LAST_NAME}" = %s
        """
        self._cursor.execute(query, ('NEWNAME', 'Ivan', 'Petrov'))
        self.assertEqual(self._cursor.rowcount, 1,
                         f"Ожидалось обновление 1 строки, обновлено {self._cursor.rowcount}")
        self._cursor.execute(
            f'SELECT "{self.COL_LAST_NAME}" FROM "{self.TEST_TABLE_NAME}" WHERE "{self.COL_FIRST_NAME}" = %s',
            ('Anna',))
        record = self._cursor.fetchone()
        self.assertIsNotNone(record)
        self.assertEqual(record[0], 'NEWNAME', f"Фамилия Анны должна была измениться на NEWNAME")

    def test_1_22_update_zero_rows(self):
        """№ 1-22 Проверка UPDATE, обновляющей 0 строк"""

        columns, values = self.get_test_data()
        self.db.insert_many(columns, values)
        self._cursor.execute(
            f'UPDATE "{self.TEST_TABLE_NAME}" SET "{self.COL_LAST_NAME}" = %s WHERE "{self.COL_FIRST_NAME}" = %s',
            ('NEWNAME', ''))
        self.assertEqual(self._cursor.rowcount, 0, "Команда UPDATE должна вернуть 0 затронутых строк")
        self._cursor.execute(f'SELECT COUNT(*) FROM "{self.TEST_TABLE_NAME}" WHERE "{self.COL_LAST_NAME}" = %s',
                             ('NEWNAME',))
        self.assertEqual(self._cursor.fetchone()[0], 0, "Записей с фамилией NEWNAME быть не должно")

    def test_1_23_delete_zero_rows(self):
        """№ 1-23 Проверка DELETE, удаляющей 0 строк"""

        columns, values = self.get_test_data()
        self.db.insert_many(columns, values)
        self._cursor.execute(f'DELETE FROM "{self.TEST_TABLE_NAME}" WHERE 0=1')
        self.assertEqual(self._cursor.rowcount, 0, "Команда DELETE должна вернуть 0 удаленных строк")
        self._cursor.execute(f'SELECT COUNT(*) FROM "{self.TEST_TABLE_NAME}"')
        self.assertEqual(self._cursor.fetchone()[0], 3, "В таблице должно остаться 3 строки")

    def test_1_24_aborted_transaction_behavior(self):
        """№ 1-24 Проверка битой транзакции (ошибка в синтаксисе)"""

        expected_sqlstate_1 = '42601'
        expected_sqlstate_2 = '25P02'
        with self.assertRaises(psycopg2.ProgrammingError) as cm:
            self._cursor.execute(f'TRUNCAT TABLE "{self.TEST_TABLE_NAME}"')
        self.assertEqual(cm.exception.pgcode, expected_sqlstate_1,
                         f"Должна возникнуть ошибка {expected_sqlstate_1}")
        with self.assertRaises(psycopg2.InternalError) as cm_aborted:
            self.db.insert_record({self.COL_FIRST_NAME: "Ivan", self.COL_LAST_NAME: "Ivanov"})
        self.assertEqual(cm_aborted.exception.pgcode, expected_sqlstate_2,
                         f"Транзакция должна быть заблокирована (код {expected_sqlstate_2})")

    def test_1_25_update_with_interval_and_between(self):
        """№ 1-25 Проверка UPDATE с изменением даты и условием BETWEEN"""

        self._cursor.execute(f"""
            INSERT INTO "{self.TEST_TABLE_NAME}" ("{self.COL_INDEX}", "{self.COL_FIRST_NAME}",
             "{self.COL_LAST_NAME}", "{self.COL_DOB}") 
            SELECT i, '"{self.COL_FIRST_NAME}"_' || i, '"{self.COL_LAST_NAME}"_' || i, '1990-01-01'
            FROM generate_series(1, 200) AS i
        """)
        self.assertEqual(self._cursor.rowcount, 200)
        self._cursor.execute(f"""
            UPDATE "{self.TEST_TABLE_NAME}" 
            SET "{self.COL_DOB}" = "{self.COL_DOB}" - INTERVAL '1 year' 
            WHERE "{self.COL_INDEX}" BETWEEN 1 AND 200
        """)
        self.assertEqual(self._cursor.rowcount, 200)
        self._cursor.execute(f'SELECT "{self.COL_DOB}" FROM "{self.TEST_TABLE_NAME}" WHERE "{self.COL_INDEX}" = 100')
        result = self._cursor.fetchone()
        self.assertEqual(str(result[0]), '1989-01-01', "Дата должна уменьшиться на 1 год")

    def test_1_26_aggregate_functions_count_avg(self):
        """№ 1-26 Проверка SELECT с агрегатными функциями (стабильный расчет)"""

        values = [
            ('Old', 'Man', '1970-01-01'),
            ('Young', 'Girl', '2000-01-01'),
            ('Middle', 'Age', '1990-01-01')
        ]
        self.db.insert_many([self.COL_FIRST_NAME, self.COL_LAST_NAME, self.COL_DOB], values)
        query = f"""
            SELECT COUNT(*) AS total_people, 
                   AVG(EXTRACT(YEAR FROM AGE(TIMESTAMP '2026-01-01', "{self.COL_DOB}"))) AS avg_age 
            FROM "{self.TEST_TABLE_NAME}"
        """
        self._cursor.execute(query)
        result = self._cursor.fetchone()
        self.assertEqual(result[0], 3)
        expected_avg = 39.33
        actual_avg = float(result[1])

        self.assertAlmostEqual(actual_avg, expected_avg, places=2,
                               msg=f"Средний возраст {actual_avg} не совпал с {expected_avg}")

    def test_1_27n_insert_null_into_not_null_column(self):
        """№ 1-27n Проверка INSERT (вставка Null в NOT NULL столбец FirstName)"""

        invalid_data = {self.COL_FIRST_NAME: None, self.COL_LAST_NAME: "Sidorova"}
        self.assertSqlError('23502', self.db.insert_record, invalid_data)

    def test_1_28_explicit_rollback_verification(self):
        """№ 1-28 Проверка ROLLBACK (физическая отмена изменений)"""

        self.db.insert_record({self.COL_FIRST_NAME: "Initial", self.COL_LAST_NAME: "User"})
        self._connection.commit()
        self._cursor.execute(f'SELECT COUNT(*) FROM "{self.TEST_TABLE_NAME}"')
        initial_count = self._cursor.fetchone()[0]
        self._cursor.execute(f'TRUNCATE TABLE "{self.TEST_TABLE_NAME}"')
        self.db.insert_record({self.COL_FIRST_NAME: "Masha", self.COL_LAST_NAME: "Sidorova"})
        self._cursor.execute(f'SELECT COUNT(*) FROM "{self.TEST_TABLE_NAME}"')
        self.assertEqual(self._cursor.fetchone()[0], 1)
        self._connection.rollback()
        self._cursor.execute(f'SELECT COUNT(*) FROM "{self.TEST_TABLE_NAME}"')
        final_count = self._cursor.fetchone()[0]
        self.assertEqual(final_count, initial_count, "Данные не восстановились после ROLLBACK")

    def get_test_data(self, index=None, custom_fname=None, with_null_dob=False, as_dict=False):
        """
        Универсальный генератор данных.
        :param index: Значение для колонки INDEX (если нужно).
        :param custom_fname: Специфическое имя (для тестов границ или поиска).
        :param with_null_dob: Флаг для вставки NULL в дату.
        :param as_dict: Если True, возвращает словарь {col: val}, иначе кортеж (cols, values).
        """

        single_data = {
            self.COL_FIRST_NAME: custom_fname if custom_fname is not None else "Petr",
            self.COL_LAST_NAME: "Petrov",
            self.COL_DOB: "1995-05-15" if not with_null_dob else None
        }
        if index is not None:
            single_data[self.COL_INDEX] = index
        if as_dict or index is not None:
            return single_data
        cols = [self.COL_FIRST_NAME, self.COL_LAST_NAME, self.COL_DOB]
        values = [
            (custom_fname or 'Ivan', 'Ivanov', '1990-01-01' if not with_null_dob else None),
            ('Petr', 'Petrov', '1985-05-12'),
            ('Anna', 'Sidorova', '1995-11-20')
        ]
        return cols, values
