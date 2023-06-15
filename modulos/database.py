from sqlalchemy import create_engine
import pyodbc
import pandas as pd
import numpy as np


class Database():

    conn = None    
    def __init__(self, db_path, db_fileName):
        self.db_path = db_path
        self.db_fileName = db_fileName
        

    def connect_access(self):              
        connStr = f'Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={self.db_path}\\{self.db_fileName};'
        Database.conn = pyodbc.connect(connStr)
        return Database.conn
    
    def connect_sqlite(self):              
        connStr = f'sqlite:///{self.db_path}\\{self.db_fileName}'
        engine = create_engine(connStr)
        Database.conn = engine.connect()
        return Database.conn
        
    @staticmethod
    def query_table_access(table):
        cursor = Database.conn.cursor()
        cursor.execute(f"select * from {table}")
        cols = []
        for count, col in enumerate(cursor.description):
            cols.append(col[0])
        records = cursor.fetchall()
        if len(records) > 0:
            df = pd.DataFrame(np.array(records), columns=cols)
        else:
            df = pd.DataFrame(np.array([np.nan] * len(cols)).reshape((1,len(cols))), columns=cols)    
        return df

    @staticmethod
    def query_table_sqlite(table):
        queryStr = (f"select * from {table}")
        sqlQuery = pd.read_sql_query(sql=queryStr, con=Database.conn)
        df = pd.DataFrame(sqlQuery)
        return df
   