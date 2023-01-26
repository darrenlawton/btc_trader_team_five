import pyodbc

class SqlDb(object):
    def __init__(self, server, db_name):
        conn_str = ("Driver=SQL Server;"
                    "Server={};"
                    "Database={};"
                    "Trusted_Connection=yes".format(server, db_name))
        self.conn = pyodbc.connect(conn_str)

    def __del__(self):
        self.conn.close()

    def query(self, sql):
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            self.conn.commit()
            return cursor.fetchall()

