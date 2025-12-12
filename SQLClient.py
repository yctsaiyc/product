import pymssql


class SQLClient:
    def __init__(self, server, database, user, password, port=1433):
        self.conn = pymssql.connect(
            server=server, port=port, user=user, password=password, database=database
        )
        self.cursor = self.conn.cursor()

    def execute(self, sql: str):
        self.cursor.execute(sql)

        # SELECT
        try:
            fetch = self.cursor.fetchall()
            print(fetch)
            return fetch

        # INSERT / UPDATE / DELETE
        # CREATE TABLE / ALTER TABLE
        except pymssql.exceptions.OperationalError:
            self.conn.commit()
            print("Done!")

    def close(self):
        self.conn.close()

    def select_all(self, table):
        sql = f"""
            SELECT * FROM {table}
        """
        print(sql)
        return self.execute(sql)

    def select_columns(self, table, schema="dbo"):
        sql = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}'
              AND TABLE_NAME = '{table}'
              ORDER BY ORDINAL_POSITION;
        """
        print(sql)
        return self.execute(sql)

    def drop_table(self, table):
        sql = f"""
            DROP TABLE {table}
        """
        print(sql)
        return self.execute(sql)
