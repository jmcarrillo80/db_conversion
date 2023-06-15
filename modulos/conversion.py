from modulos.database import Database
from modulos import encoder
import json


class Conversion():

    def __init__(self, db_path, db_fileName):
        self.db_path = db_path
        self.db_fileName = db_fileName
        

    def extractDataAccess(self, table):
        db = Database(self.db_path, self.db_fileName)
        conn = db.connect_access()
        df = db.query_table_access(table)
        conn.close()
        return df

    def extractDataSqlite(self, table):
        db = Database(self.db_path, self.db_fileName)
        conn = db.connect_sqlite()
        df = db.query_table_sqlite(table)
        conn.close()
        return df

    @staticmethod
    def parquetFile(df, directory_output, fileName, table):
        parquet_file = f"{directory_output}/{fileName}__{table}.parquet"
        df.to_parquet(parquet_file, engine='pyarrow', compression = 'gzip')

    @staticmethod
    def jsonFile(df, directory_output, fileName, table):
        json_file = f"{directory_output}/{fileName}__{table}.json"
        key_dict = df.columns.to_list()
        records = []
        for i in range(0, len(df)):
            record = {k: v for k, v in zip(key_dict, df.iloc[i].reset_index(drop=True))}
            records.append(record)

        with open(json_file, 'w') as f:
            json.dump(records, f, indent=4, cls=encoder.NpEncoder)