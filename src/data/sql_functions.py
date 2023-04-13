# Libraries to import
import pyodbc
import re
import pandas as pd

# Global variables
DATA_TYPE_MAPPING = {
        'int64'     : 'bigint'
    ,   'float64'   : 'float'
    ,   'bool'      : 'boolean'
    ,   'datetime64[ns, UTC]'   :   'datetime'
    ,   'datetime64[ns]'        :   'datetime'
}


def get_column_types(pd_data_types):
    sql_data_types = []

    for c in pd_data_types:
        sql_data_types.append(DATA_TYPE_MAPPING.get(str(c), "varchar (255)"))

    return sql_data_types

class SqlDb(object):
    def __init__(self, server, db_name):
        conn_str = ("Driver=SQL Server;"
                    "Server={};"
                    "Database={};"
                    "Trusted_Connection=yes".format(server, db_name))
        self.database = db_name
        
        try:
            self.conn = pyodbc.connect(conn_str)
        except Exception as error:
            print("Could not establish connection to {} due to: {}".format(server, error))

    def __del__(self):
        self.conn.close()

    def query(self, sql, read_query = False):
        '''
        To complete
        '''
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            if read_query: 
                rows = cursor.fetchall()
                values = [list(row) for row in rows]
                column_names = [column[0] for column in cursor.description]
                return pd.DataFrame(values, columns=column_names)
            else: self.conn.commit()

    def create_table(self, table_name, dataframe):
        '''
        To complete
        '''
        self.query("DROP TABLE IF EXISTS {}".format(table_name))
        
        # Get column names and types
        column_names = [re.sub('[^A-Za-z0-9]+', '_', c) for c in list(dataframe.columns.values)]
        column_types = get_column_types(dataframe.dtypes)

        # Create new table
        create_table_query = "CREATE TABLE " + table_name + " ("
        for i in range(len(column_names)):
            create_table_query = create_table_query + '\n' + column_names[i] + ' ' + column_types[i] + ','
        create_table_query = create_table_query[:-1] + " );"
        
        try: 
            self.query(create_table_query)
            self.update_table(table_name, dataframe, column_names)
        except Exception as error:
            print("An error occured whilst creating and updating table {}: {}".format(table_name, error))

    def update_table(self, table_name, dataframe, column_names):
        '''
        To complete
        '''
        # Convert na values to NULL for SQL 
        dataframe.fillna("NULL", inplace=True)
        # Convert timestamp format to a date string
        date_cols_to_reformat = [i for i, x in enumerate(dataframe.dtypes) if x in ["datetime64[ns, UTC]", "datetime64[ns]"]]
    
        for index, row in dataframe.iterrows():
            update_values = row.values
            for c in date_cols_to_reformat:
                update_values[c] = "'" + update_values[c].strftime('%Y-%m-%d %X') + "'"
            
            update_table_query = "INSERT INTO {} ({}) values({})".format(table_name, ','.join(column_names), ','.join([str(n) for n in update_values]))
            self.query(update_table_query)
        
        # confirm transaction 
        confirmation_query = "SELECT count(*) from {};".format(table_name)
        print("{} of {} rows have successfully been writen to {}".format(self.query(confirmation_query, True).iloc[0].values[0], dataframe.shape[0], table_name))

