from modulos.conversion import Conversion
import os
import json

directory = input(r"Enter the path of the databases folder: ")
db_fileFormat = input(r"Enter the database file format(mdb/accdb): ")
output_fileFormat = input(r"Enter the output file format(parquet/json): ")
print(f"Files in the directory: {directory}")
 
files = os.listdir(directory)
files = [f for f in files if os.path.isfile(f"{directory}/{f}") and f.endswith(f".{db_fileFormat}")]
print(*files, sep="\n")

for db_file in files:
    convObj = Conversion(dbPath=directory, dbName=db_file)
    with open('databases_tables.json') as f:
        db_dict = json.load(f)
    for db in db_dict:
        if db['database']==db_file.split(".")[0]:
            df = convObj.extractData(db['table'])
            if output_fileFormat=='parquet':
                convObj.parquetFile(df, db_file.split(".")[0], db['table'])
            elif output_fileFormat=='json':
                convObj.jsonFile(df, db_file.split(".")[0], db['table'])





