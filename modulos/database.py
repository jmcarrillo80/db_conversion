import pyodbc
import pandas as pd
import numpy as np


class Database():

    conn = None
    def __init__(self, dbPath, dbName):
        self.dbPath = dbPath
        self.dbName = dbName
        

    def connect(self):              
        connStr = f'Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={self.dbPath}\\{self.dbName};'
        Database.conn = pyodbc.connect(connStr)
        return Database.conn
        
    @staticmethod
    def query_table(table):
        cursor = Database.conn.cursor()
        cursor.execute(f"select * from {table}")
        cols = []
        for count, col in enumerate(cursor.description):
            cols.append(col[0])
        records = cursor.fetchall()
        df = pd.DataFrame(np.array(records), columns=cols)
        return df

   