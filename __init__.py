import logging
import sqlalchemy
import azure.functions as func
from azure.storage.blob import BlobClient
import pyodbc
import pandas as pd
from configparser import ConfigParser
import io
from io import BytesIO
from datetime import datetime

def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")
    
    
    config_parser = ConfigParser()
    config_parser.read('config.ini')

    db_user = config_parser.get('SQL','Utilizator')
    db_pass = config_parser.get('SQL','Parola')
    blob_connection = config_parser.get('Blob','CONNECTION_STRING')

    SERVER = 'azr8846devsql01.database.windows.net'
    DATABASE ='azr8846devsql01-db1'
    DRIVER1 = 'ODBC Driver 17 for SQL Server'
    USERNAME = db_user
    PASSWORD = db_pass
    CONNECTION_STRING = blob_connection
    
    database_connection = f'mssql://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}?driver={DRIVER1}'
    

    engine = sqlalchemy.create_engine(database_connection)

    cursor = engine.connect()

    df_logs = pd.read_sql_table(table_name='EMOPAA_Logs',con = cursor,schema='ODS')

    cursor.close()
           
    conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                        'Server=azr8846devsql01.database.windows.net;'
                        'Database=azr8846devsql01-db1;'
                        'Uid=it-pond-usr;'
                        'Pwd=wkt2%QyHuc;'
                        'Encrypt=yes;'
                        'TrustServerCertificate=no;'
                        'Connection Timeout=30;')

    cursor = conn.cursor()

    bc = BlobClient.from_connection_string(blob_connection
    , container_name='verifyid',blob_name= myblob.name.split('/',1)[1])

    df = pd.read_excel(BytesIO(bc.download_blob().readall()))
    
    columnSupplier = 'Supplier'
    if columnSupplier in df.columns:
        dateSql = datetime.today().strftime("%Y-%m-%d")
        for i in df['Supplier'].unique():
            fileName = str(i) + '.xlsx'
            cursor.execute(f'''UPDATE ODS.EMOPAA_Logs SET [Replied]=1 WHERE Supplier = {i}''')
            conn.commit()  
            cursor.execute(f'''UPDATE ODS.EMOPAA_Logs SET [Reminder]=0 WHERE Supplier = {i}''')
            conn.commit()
            query1 = 'UPDATE ODS.EMOPAA_Logs SET [Received fileName]= ? WHERE Supplier = ?'
            cursor.execute(query1, (fileName, int(i))) 
            conn.commit()  
            query = 'UPDATE ODS.EMOPAA_Logs SET [ReturnDate]= ? WHERE Supplier = ?'
            cursor.execute(query, (dateSql, int(i)))
            conn.commit()  
            output = io.BytesIO()
        with  pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            bc1 = BlobClient.from_connection_string(CONNECTION_STRING
                    , container_name='receivedfiles'
                    , blob_name=f'{i}.xlsx')
            df.to_excel(writer,sheet_name=f'{i}-{dateSql}.xlsx',index=False)
            writer.save()
            xlsx_data = output.getvalue()
            bc1.upload_blob(data=xlsx_data)
        bc.delete_blob()

    else:
        with  pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            bc1 = BlobClient.from_connection_string(CONNECTION_STRING
                    , container_name='wrongfiles'
                    , blob_name=f'{i}.xlsx')
        df.to_excel(writer,sheet_name=f'{i}-{dateSql}.xlsx',index=False)
        writer.save()
        xlsx_data = output.getvalue()
        bc1.upload_blob(data=xlsx_data)
        bc.delete_blob()
