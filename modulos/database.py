import pyodbc
import pandas as pd


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
        df = pd.read_sql_query(f"select * from {table}", Database.conn)
        return df

   