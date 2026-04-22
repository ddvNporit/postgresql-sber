class DbActions:
    def __init__(self, cursor, table_name):
        self.cursor = cursor
        self.table = f'"{table_name}"'

    def insert_record(self, data: dict):
        """Универсальная вставка: принимает словарь {колонки: значения}"""

        columns = ", ".join([f'"{k}"' for k in data.keys()])
        placeholders = ", ".join(["%s"] * len(data))
        sql = f'INSERT INTO {self.table} ({columns}) VALUES ({placeholders})'
        self.cursor.execute(sql, list(data.values()))
        return self.cursor.rowcount

    def get_column_types(self, columns: list, lookup_data: dict):
        """Универсальная проверка типов"""

        select_clause = ", ".join([f'CONCAT("{c}", \': \', pg_typeof("{c}"))' for c in columns])
        where_clause = " AND ".join([f'"{k}" = %s' for k in lookup_data.keys()])

        sql = f'SELECT {select_clause} FROM {self.table} WHERE {where_clause}'
        self.cursor.execute(sql, list(lookup_data.values()))
        return self.cursor.fetchone()

    def get_extracted_part(self, part: str, column: str, lookup_col: str, lookup_val: str):
        """Универсальный EXTRACT (YEAR, MONTH и т.д.)"""

        sql = f'SELECT EXTRACT({part} FROM "{column}") FROM {self.table} WHERE "{lookup_col}" = %s'
        self.cursor.execute(sql, (lookup_val,))
        return self.cursor.fetchone()

    def insert_many(self, columns: list, values_list: list):
        """Массовая вставка записей"""

        cols_str = ", ".join([f'"{c}"' for c in columns])
        placeholders = ", ".join(["%s"] * len(columns))
        sql = f'INSERT INTO {self.table} ({cols_str}) VALUES ({placeholders})'
        self.cursor.executemany(sql, values_list)
        return self.cursor.rowcount
