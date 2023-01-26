# Libraries to import
import pyodbc


# Global variables
DATA_TYPE_MAPPING = {
        'int64'     : 'int'
    ,   'float64'   : 'float'
    ,   'bool'      : 'boolean'
    ,   'datetime64[ns, UTC]'   :   'datetime'
}


def get_column_types(pd_data_types):
    sql_data_types = []

    for c in pd_data_types:
        sql_data_types.append(DATA_TYPE_MAPPING.get(str(c), "varchar"))

    return sql_data_types

class SqlDb(object):
    def __init__(self, server, db_name):
        conn_str = ("Driver=SQL Server;"
                    "Server={};"
                    "Database={};"
                    "Trusted_Connection=yes".format(server, db_name))
        self.database = db_name
        self.conn = pyodbc.connect(conn_str)

    def __del__(self):
        self.conn.close()

    def query(self, sql, read_query = False):
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            self.conn.commit()
            if read_query: return cursor.fetchall()

    def create_table(self, table_name, dataframe):

        self.query("USE {} DROP TABLE IF EXISTS {}".format(self.database, table_name))
        
        # Get column names and types
        column_names = list(dataframe.columns.values)
        column_types = get_column_types(dataframe.dtypes)
        
        # Create new table
        create_table_query = "USE " + self.database + " CREATE TABLE " + table_name + " ("
        for i in range(len(column_names)):
            create_table_query = create_table_query + '\n' + column_names[i].replace(" ", "_") + ' ' + column_types[i] + ','
        create_table_query = create_table_query[:-1] + " );"

        return self.query(create_table_query)



