from modulos.database import Database
from modulos import encoder
import json


class Conversion():

    def __init__(self, dbPath, dbName):
        self.dbPath = dbPath
        self.dbName = dbName
        

    def extractData(self, table):
        db = Database(self.dbPath, self.dbName)
        conn = db.connect()
        df = db.query_table(table)
        conn.close()
        return df

    @staticmethod
    def parquetFile(df, fileName, table):
        parquet_file = f"outputFiles/{fileName}__{table}.parquet"
        df.to_parquet(parquet_file, engine='pyarrow', compression = 'gzip')

    @staticmethod
    def jsonFile(df, fileName, table):
        json_file = f"outputFiles/{fileName}__{table}.json"
        key_dict = df.columns.to_list()
        records = []
        for i in range(0, len(df)):
            record = {k: v for k, v in zip(key_dict, df.iloc[i].reset_index(drop=True))}
            records.append(record)

        with open(json_file, 'w') as f:
            json.dump(records, f, indent=4, cls=encoder.NpEncoder)