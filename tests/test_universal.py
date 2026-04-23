"""
A testing framework for checking PostgreSQL DML operations
author: Dmitry Denisov
GitHub: https://github.com/ddvNporit/postgresql-sber
Email: dima.nporit@gmail.com
"""

from db.base_test import PostgreSQLTestCase
from db.helpers import DbActions


class TestUniversalDML(PostgreSQLTestCase):

    def setUp(self):
        super().setUp()
        self.db = DbActions(self._cursor, self.TEST_TABLE_NAME)
        self.payload = self._prepare_payload()

    def _prepare_payload(self):
        """Вспомогательный метод для подготовки тестовых данных"""

        payload = {}
        for col in self.table_schema:
            is_serial = 'serial' in col['type'].lower() or (
                    col['has_default'] and 'nextval' in str(col['name']).lower())
            if is_serial:
                continue
            val = self.generate_test_value(col)
            if val is not None:
                payload[col['name']] = val
        return payload

    def test_generic_lifecycle(self):
        """Универсальный CRUD цикл: INSERT -> SELECT -> UPDATE -> DELETE"""

        if not self.payload:
            self.skipTest(f"Не удалось определить столбцы для таблицы {self.TEST_TABLE_NAME}")

        # --- 1. INSERT ---
        rows_inserted = self.db.insert_record(self.payload)
        self.assertEqual(rows_inserted, 1, "Ошибка при выполнении INSERT")
        print(f"✅ INSERT: Запись успешно добавлена в '{self.TEST_TABLE_NAME}'")

        # --- 2. SELECT ---
        filter_col = list(self.payload.keys())[0]
        filter_val = self.payload[filter_col]
        sql_select = f'SELECT * FROM "{self.TEST_TABLE_NAME}" WHERE "{filter_col}" = %s'
        self._cursor.execute(sql_select, (filter_val,))
        result = self._cursor.fetchone()
        self.assertIsNotNone(result, f"Запись не найдена через SELECT")
        print(f"✅ SELECT: Запись успешно найдена в '{self.TEST_TABLE_NAME}'")

        # --- 3. UPDATE ---
        update_col_name = list(self.payload.keys())[-1]
        col_meta = next(c for c in self.table_schema if c['name'] == update_col_name)
        original_val = self.payload[update_col_name]
        db_type = col_meta['type'].lower()

        if 'date' in db_type or 'time' in db_type:
            new_val = "2026-12-31"
        elif 'int' in db_type or 'serial' in db_type or 'numeric' in db_type:
            new_val = original_val + 1
        elif 'bool' in db_type:
            new_val = not original_val
        else:
            length = col_meta['max_len'] or 10
            new_val = f"U_{original_val}"[:length]

        sql_update = f'UPDATE "{self.TEST_TABLE_NAME}" SET "{update_col_name}" = %s WHERE "{filter_col}" = %s'
        self._cursor.execute(sql_update, (new_val, filter_val))
        self.assertEqual(self._cursor.rowcount, 1, "Ошибка при выполнении UPDATE")
        self._cursor.execute(f'SELECT "{update_col_name}" FROM "{self.TEST_TABLE_NAME}" WHERE "{filter_col}" = %s',
                             (filter_val,))
        updated_val_in_db = self._cursor.fetchone()[0]
        self.assertEqual(str(updated_val_in_db), str(new_val),
                         "Значение в БД не совпадает с обновленным")
        print(f"✅ UPDATE: Поле '{update_col_name}' ({db_type}) успешно обновлено в '{self.TEST_TABLE_NAME}'")

        # --- 4. DELETE ---
        sql_delete = f'DELETE FROM "{self.TEST_TABLE_NAME}" WHERE "{filter_col}" = %s'
        self._cursor.execute(sql_delete, (filter_val,))
        self.assertEqual(self._cursor.rowcount, 1, "Ошибка при выполнении DELETE")
        self._cursor.execute(sql_select, (filter_val,))
        self.assertIsNone(self._cursor.fetchone(), "Запись все еще существует после DELETE")
        print(f"✅ DELETE: Запись успешно удалена из '{self.TEST_TABLE_NAME}'")
