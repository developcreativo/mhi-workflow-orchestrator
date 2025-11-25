from sqlalchemy import create_engine
import pandas as pd
import datetime
from utils import  controller

hostname = '77.228.143.198'
port = 10250
user = "mindcloud"
password = "M1nd+33"
dbname = "AntforHotel-FERGUSIB"
#table_names = ['HOTHoteles','RECReservas','RECCardex']
table_names = ['HOTHoteles','HOTEntidadesNegocio']
schema = 'dbo'

ini = datetime.datetime.now()
print("Inicio: ", ini)

engine = create_engine(f"mssql+pymssql://{user}:{password}@{hostname}:{port}/{dbname}", echo=False)
print (f'Leeremos todas estas tablas: {table_names} de la base de datos {dbname}')
for table_name in table_names:
    print(f"INICIO DE CARGA TABLA: {table_name}")
    try:
        df = pd.read_sql_table(table_name=table_name, con=engine, schema=schema)
        #df.drop(columns=['UUID'])
        print(list(df.columns))
        print(df)

        # controller.saveStorage(
        #    account='fergus',
        #    datasource="avalon",
        #    dataset=table_name,
        #    data_df=df
        # )
        print(f"FIN DE CARGA TABLA: {table_name}")
        

    except Exception as e:
        print(f'ERROR al Leer la base de datos de SQL SERVER.... jiji era lo que esperaba si no tengo acceso....:{type(e)}: {e}.')
        print(f'ERROR: {e}.')
        print(f'type: {type(e)}')
             
fin = datetime.datetime.now()
print("Fin: ", fin)
print("Duración: ", fin.timestamp()-ini.timestamp())

