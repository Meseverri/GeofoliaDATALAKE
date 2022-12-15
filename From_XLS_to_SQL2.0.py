import pandas as pd
import numpy as np
import datetime
import os
import sqlalchemy
from sqlalchemy.engine import URL

"""XLS Files  Manipulations functions"""
def Normalization(df,columnsToNorm,PKpos=-1):
    # df original Dataframe
    # columnsToNorm that we want to Normalize
    data=df[columnsToNorm]
    data=data.drop_duplicates(columnsToNorm)
    data=data.dropna(axis=0,how="all")
    if PKpos>=0:
        Column=data[columnsToNorm[PKpos]]
        data=data.drop(columns=columnsToNorm[PKpos])
        data.insert(loc=0,column=columnsToNorm[PKpos],value=Column)
        data=data.reset_index(drop=True)
    return data
def XLS_ReportPathFinder(folderpath="C:/Users/mesev\Documents/BackUp/TRABAJO/BODEGA DE OTAZU/Tareas Geofolia/Data Geofolia\Geofolia DATALAKE"):
    GeofReportFile=""
    for i in os.listdir(folderpath):
        if i[:17]=="Parcelas y partes":
            print(i)
            GeofReportFile+=i
    return GeofReportFile
def codigoVariedad(variedad):
    CodigoVariedad={"Berúes":"00000001",
    "Barbecho":"00000000",
    "Cabernet Franc":"10900042",
    "Chardonnay":"10900068",
    "Cabernet Sauvignon":"10900043",
    "Varias":"00000003",
    "Garnacha Tinta":"10900097",
    "Merlot":"10900134",
    "Pinot Noir":"10900169",
    "Tempranillo":"10900200"
    }
    try:
        ret=CodigoVariedad[variedad]
    except:
        ret=None
    return ret
"""Functions for Azure SQL interactions"""
def SQLengine( server = 'servidorotazu.database.windows.net' , database = 'dbotazu' ,username = 'otazu', password = 'Bodega2022!' ):
     connection_string = "DRIVER={SQL Server};"
     connection_string += f"SERVER={server};DATABASE={database};UID={username};PWD={password}"
     
     connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
     engine = sqlalchemy.create_engine(connection_url)
     return engine
def getSQL_t(tableName,engine):
     # Regresa una lista de tuplas, cada una con la filas de informacion en SQL
     result = engine.execute(f"SELECT * from {tableName}")
     data=[]
     print(f"Table Name: {tableName}")
     for row in result:
          Trow=[]
          for e in row:
               Trow.append(e.strip())
          data.append(tuple(Trow))
     result.close()
     return data

"""-------------Find the Parcelas y partes xlsx-------------"""
AT0=datetime.datetime.now()
folderPath="C:/Users/mesev/Documents/BackUp/TRABAJO/BODEGA DE OTAZU/Tareas Geofolia/Data Geofolia/Geofolia DATALAKE/GeofoliaDATALAKE"

filePath=XLS_ReportPathFinder(folderPath)

"""--Dictionario almacanando el nombre da las tablas junto con la tabla nueva correspondiente--"""
newTablesDict={}
timeDict={}

"""------------------Diario de Parcelas---------------------"""
newParcelColNames={'Campaña':"CAMPAÑA",
                    'Razón social': 'RAZÓN_SOCIAL',
                    'Nombre parcela': 'NOMBRE_PARCELA',
                    'Superf (ha)': 'SUPERF_HA',
                    'Tipo de suelo': 'TIPO_DE_SUELO',
                    'Código Tipo de suelo': 'CÓDIGO_TIPO_DE_SUELO',
                    'Cultivo': 'CULTIVO',
                    'Cultivo referencial': 'CULTIVO_REFERENCIAL',
                    'Código Cultivo referencial': 'CÓDIGO_CULTIVO_REFERENCIAL',
                    'Código Variedad': 'CÓDIGO_VARIEDAD',
                    'Variedad': 'VARIEDAD',
                    'Rendimiento objetivo': 'RENDIMIENTO_OBJETIVO',
                    'Unidad de rdt obj.': 'UNIDAD_DE_RDT_OBJ',
                    'Rendimiento realizado': 'RENDIMIENTO_REALIZADO',
                    'Unidad de rdt real.': 'UNIDAD_DE_RDT_REAL',
                    'Fecha de implantación': 'FECHA_DE_IMPLANTACIÓN',
                    'Fecha de cosecha': 'FECHA_DE_COSECHA'}

S_ParcelCOlumns=["Campaña",
                "Razón social",
                "Nombre parcela",
                "Superf (ha)",
                "Tipo de suelo",
                "Código Tipo de suelo",
                "Cultivo",
                "Cultivo referencial",
                "Código Cultivo referencial",
                "Código Variedad",
                "Variedad",
                "Rendimiento objetivo",
                "Unidad de rdt obj.",
                "Rendimiento realizado",
                "Unidad de rdt real.",
                "Fecha de implantación",
                "Fecha de cosecha"
]

parcelsColDict={"geo_implantacion":['NOMBRE_PARCELA','FECHA_DE_IMPLANTACIÓN',"COD_CAMPAÑA_PARCELA",2],
            "geo_variedad":["CÓDIGO_VARIEDAD","VARIEDAD",0],
            "geo_cosecha":["CAMPAÑA","NOMBRE_PARCELA","RENDIMIENTO_OBJETIVO","UNIDAD_DE_RDT_OBJ","FECHA_DE_COSECHA","COD_CAMPAÑA_PARCELA",5],
            "geo_tipos_de_suelo":["TIPO_DE_SUELO","CÓDIGO_TIPO_DE_SUELO",1],
            "geo_cultivo":["CULTIVO_REFERENCIAL","CÓDIGO_CULTIVO_REFERENCIAL",1],
            "geo_parcela":["CAMPAÑA","RAZÓN_SOCIAL","NOMBRE_PARCELA","SUPERF_HA","CÓDIGO_TIPO_DE_SUELO","CULTIVO","CÓDIGO_CULTIVO_REFERENCIAL","CÓDIGO_VARIEDAD","COD_CAMPAÑA_PARCELA",8]
            }
parcelas=pd.read_excel(filePath ,sheet_name="Parcela")[S_ParcelCOlumns]
parcelas.rename(columns = newParcelColNames, inplace = True)
parcelas["CAMPAÑA"]=parcelas["CAMPAÑA"].astype(str)
parcelas["COD_CAMPAÑA_PARCELA"]=parcelas["CAMPAÑA"]+"-"+parcelas["NOMBRE_PARCELA"]
parcelas["CAMPAÑA"]=parcelas["CAMPAÑA"].astype(np.int64)

parcelas["CÓDIGO_VARIEDAD"]=parcelas["VARIEDAD"].map(codigoVariedad)

for i in parcelsColDict:
    t0=datetime.datetime.now()
    print(i)
    newTablesDict[i]=Normalization(parcelas,parcelsColDict[i][:-1],parcelsColDict[i][-1])
    timeDict[i]=[datetime.datetime.now()-t0]
  
"""------------------Diario de Partes---------------------"""
newPartesColName={'Campaña':"CAMPAÑA",
                    'Fecha inicio': 'FECHA_INICIO',
                    'Fecha fin': 'FECHA_FIN',
                    'Hora de inicio': 'HORA_DE_INICIO',
                    'Hora fin': 'HORA_FIN',
                    'Duración (h)': 'DURACIÓN_H',
                    'Tarea': 'TAREA',
                    'Categoría de la tarea': 'CATEGORÍA_DE_LA_TAREA',
                    'Estado': 'ESTADO',
                    'Nombre de la parcela': 'NOMBRE_PARCELA',
                    'Superficie trabajada': 'SUPERFICIE_TRABAJADA',
                    'Tipo de': 'TIPO_DE',
                    'Volumen de caldo (hl)': 'VOLUMEN_DE_CALDO_HL',
                    'Tipo de familia': 'TIPO_DE_FAMILIA',
                    'Nombre específico': 'NOMBRE_ESPECÍFICO',
                    'Cantidad': 'CANTIDAD',
                    'Unidad': 'UNIDAD',
                    'Coste (€)': 'COSTE_€',
                    'Materia activa': 'MATERIA_ACTIVA'}

prioridadList=["Campaña",
                "Fecha inicio",
                "Fecha fin",
                "Hora de inicio",
                "Hora fin",
                "Duración (h)",
                "Tarea",
                "Categoría de la tarea",
                "Estado",
                "Nombre de la parcela",
                "Superficie trabajada",
                "Tipo de",
                "Volumen de caldo (hl)",
                "Tipo de familia",
                "Nombre específico",
                "Cantidad",
                'Unidad',
                "Coste (€)",
                "Materia activa"
]
t0=datetime.datetime.now()
Partes=pd.read_excel(filePath,sheet_name="Parte")[prioridadList]
timeDict["geo_partes"]=[datetime.datetime.now()-t0]
Partes.rename(columns = newPartesColName, inplace = True)

Partes["CAMPAÑA"]=Partes["CAMPAÑA"].astype(str)
Partes["COD_CAMPAÑA_PARCELA"]=Partes["CAMPAÑA"]+"-"+Partes["NOMBRE_PARCELA"]
Partes["CAMPAÑA"]=Partes["CAMPAÑA"].astype(np.int64)

Column=Partes["COD_CAMPAÑA_PARCELA"]
Partes=Partes.drop(columns="COD_CAMPAÑA_PARCELA")
Partes.insert(loc=0,column="COD_CAMPAÑA_PARCELA",value=Column)
# Agrupamos las tres tablas procedentes de los tipo de y Nombre especifco
Tipo_de=["TIPO_DE","NOMBRE_ESPECÍFICO"]
tipo_de=Normalization(Partes,Tipo_de)

Grupos_tipo_de={"Maquinaria":"geo_maquinaria",
"Materia Prima":"geo_materia_prima",
"Mano de obra":"geo_mano_obra"}
for i in Grupos_tipo_de:
    t0=datetime.datetime.now()
    newTablesDict[Grupos_tipo_de[i]]=tipo_de[tipo_de["TIPO_DE"]==i].set_index("TIPO_DE").reset_index(drop=True)
    timeDict[Grupos_tipo_de[i]]=[datetime.datetime.now()-t0]
newTablesDict["geo_partes"]=Partes


"""-------------------------------------------geting SQL Old Data---------------------------------------------------------"""
GeofoliaTables={'geo_cosecha':True, 
'geo_cultivo':False, 
'geo_implantacion':False, 
'geo_mano_obra':False, 
'geo_maquinaria':False, 
'geo_materia_prima':False, 
'geo_tipos_de_suelo':False, 
'geo_variedad':False,
'geo_parcela':True,
'geo_partes':True
}


oldTablesDict={}
# newTablesDict["geo_cosecha"].to_sql("geo_cosecha",engine,if_exists="replace",index=False,method="multi")

engine=SQLengine()
for i in GeofoliaTables:
    print(f"------------------------------------- {i} -------------------------------------------")
    print("-------------------------------------OLD-------------------------------------------")
    t0=datetime.datetime.now()
    oldTablesDict[i]=pd.read_sql_table(i, engine)
    timeDict[i].append(datetime.datetime.now()-t0)
    print(oldTablesDict[i].dtypes)
    print("-------------------------------------NEW-------------------------------------------")
    print(newTablesDict[i].dtypes)
    print(oldTablesDict[i].equals(newTablesDict[i]))
"""-------------------------------------------Pushing SQL new Data---------------------------------------------------------"""
# for i in GeofoliaTables:
#     print(f"------------------------------------- {i} -------------------------------------------")
#     t0=datetime.datetime.now()
#     equals=newTablesDict[i].equals(oldTablesDict[i])
#     if not equals:
#         if i!= "geo_partes":
#             newTablesDict[i].to_sql(i,engine,if_exists="replace",index=False,method="multi")
#         else:
#             # chunk=int(round(len(newTablesDict[i])/500,0))
#             chunk=50
#             newTablesDict[i].to_sql(i,engine,if_exists="replace",index=False,chunksize=chunk,method="multi")
#     print(datetime.datetime.now()-t0)
#     timeDict[i].append(datetime.datetime.now()-t0)

campaña=newTablesDict["geo_parcela"]["CAMPAÑA"].max()
print(campaña)
for table in GeofoliaTables:
    MDsqlalchemy=sqlalchemy.MetaData(bind=engine)
    sqlalchemy.MetaData.reflect(MDsqlalchemy)
    print(f"------------------------------------- {table} -------------------------------------------")
    equals=newTablesDict[table].equals(oldTablesDict[table])
    if not GeofoliaTables[table] :
        print(f"table {table} not transaccional")
        if not equals: newTablesDict[table].to_sql(table,engine,if_exists="replace",index=False,method="multi")
        else: continue
    else:
        print(f"table {table} transaccional")
        if table!='geo_partes':
            old_campaign=oldTablesDict[table][oldTablesDict[table]["CAMPAÑA"]==campaña]
            old_campaign.reset_index(drop=True)
            if old_campaign.equals(newTablesDict[table]):
                print(f"table {table} is the same")
                continue 
            else:
                print(f"table {table} modification")
                # query=f"DELETE FROM {table} WHERE CAMPAÑA = {campaña}"
                Q=MDsqlalchemy.tables[table]

                dele=Q.delete().where(Q.c.CAMPAÑA >= campaña )
                print(dele)
                engine.execute(dele)
                newTablesDict[table].to_sql(table,engine,if_exists="append",index=False,method="multi")
        else:
            LastDate=oldTablesDict["geo_partes"]["FECHA_INICIO"].max()
            currenteCampaingOldData=oldTablesDict[table][oldTablesDict[table]["CAMPAÑA"]==campaña]
            currenteCampaingOldData.reset_index(drop=True)
            if currenteCampaingOldData.equals(newTablesDict[table][newTablesDict[table]["FECHA_INICIO"]<=LastDate]):
                newDF=newTablesDict[table][newTablesDict[table]["FECHA_INICIO"]>LastDate]
                newDF.to_sql(table,engine,if_exists="append",index=False,method="multi")
            
            elif currenteCampaingOldData.equals(newTablesDict[table][newTablesDict[table]["FECHA_INICIO"]<=LastDate-datetime.timedelta(weeks=4)]):
                Q=MDsqlalchemy.tables[table]
                dele=Q.delete().where(Q.c.FECHA_INICIO >= LastDate-datetime.timedelta(weeks=4)) 
                print(dele)
                engine.execute(dele)
                newDF=newTablesDict[table][newTablesDict[table]["FECHA_INICIO"]>LastDate-datetime.timedelta(weeks=4)]
                newDF.to_sql(table,engine,if_exists="append",index=False,method="multi")
            else:
                print(f"{table} New campaign massive upload")
                Q=MDsqlalchemy.tables[table]
                dele=Q.delete().where(Q.c.CAMPAÑA == campaña )
                print(dele)
                engine.execute(dele)
                chunk=50
                newTablesDict[i].to_sql(i,engine,if_exists="append",index=False,chunksize=chunk,method="multi")


completeCOmpelationTime=datetime.datetime.now()-AT0
print("-----------------------------Compilation Time:----------------------\n",completeCOmpelationTime)
# ReplaceTime=pd.DataFrame(timeDict,index=["Normalizing Time", "Old Table SQL TIME","SQL Query Time"])

# ReplaceTime.T.to_excel("ManipulationTime.xlsx")
