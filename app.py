from modulos.conversion import Conversion
import sys
import json


def main(directory_db, db_name, db_fileFormat, tables, directory_output, output_fileFormat):       
    tables = tables.replace(str("'"), str('"'))
    tables = json.loads(str(tables))
    db_file = f"{db_name}.{db_fileFormat}"

    print(f"Tables from database '{db_file}' converted:")
    convObj = Conversion(db_path=directory_db, db_fileName=db_file)
    for table in tables:
        if db_fileFormat == 'mdb':
            df = convObj.extractDataAccess(table)
        elif db_fileFormat == 'db':
            df = convObj.extractDataSqlite(table)
        if output_fileFormat == 'parquet':
            convObj.parquetFile(df=df, directory_output=directory_output, fileName=db_name, table=table)
            print(f"\t -{table} --> parquet file")
        elif output_fileFormat == 'json':
            convObj.jsonFile(df=df, directory_output=directory_output, fileName=db_name, table=table)
            print(f"\t -{table} --> json file")

  

if __name__== "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])



