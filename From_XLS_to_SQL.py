import pandas as pd
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
folderPath="C:/Users/mesev/Documents/BackUp/TRABAJO/BODEGA DE OTAZU/Tareas Geofolia/Data Geofolia/Geofolia DATALAKE"

filePath=XLS_ReportPathFinder(folderPath)

"""--Dictionario almacanando el nombre da las tablas junto con la tabla nueva correspondiente--"""
newTablesDict={}
timeDict={}

"""------------------Diario de Parcelas---------------------"""
newParcelColNames={'Razón social': 'RAZÓN_SOCIAL',
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

S_ParcelCOlumns=["Razón social",
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

parcelsColDict={"geo_implantacion":['NOMBRE_PARCELA','FECHA_DE_IMPLANTACIÓN',0],
            "geo_variedad":["CÓDIGO_VARIEDAD","VARIEDAD",0],
            "geo_cosecha":["NOMBRE_PARCELA","RENDIMIENTO_OBJETIVO","UNIDAD_DE_RDT_OBJ","FECHA_DE_COSECHA",0],
            "geo_tipos_de_suelo":["TIPO_DE_SUELO","CÓDIGO_TIPO_DE_SUELO",1],
            "geo_cultivo":["CULTIVO_REFERENCIAL","CÓDIGO_CULTIVO_REFERENCIAL",1],
            "geo_parcela":["RAZÓN_SOCIAL","NOMBRE_PARCELA","SUPERF_HA","CÓDIGO_TIPO_DE_SUELO","CULTIVO","CÓDIGO_CULTIVO_REFERENCIAL","CÓDIGO_VARIEDAD",1]
            }
parcelas=pd.read_excel(filePath ,sheet_name="Parcela")[S_ParcelCOlumns]
parcelas.rename(columns = newParcelColNames, inplace = True)
parcelas["CÓDIGO_VARIEDAD"]=parcelas["VARIEDAD"].map(codigoVariedad)

for i in parcelsColDict:
    t0=datetime.datetime.now()
    newTablesDict[i]=Normalization(parcelas,parcelsColDict[i][:-1],parcelsColDict[i][-1])
    timeDict[i]=[datetime.datetime.now()-t0]

        
"""------------------Diario de Partes---------------------"""
newPartesColName={'Fecha inicio': 'FECHA_INICIO',
                    'Fecha fin': 'FECHA_FIN',
                    'Hora de inicio': 'HORA_DE_INICIO',
                    'Hora fin': 'HORA_FIN',
                    'Duración (h)': 'DURACIÓN_H',
                    'Tarea': 'TAREA',
                    'Categoría de la tarea': 'CATEGORÍA_DE_LA_TAREA',
                    'Estado': 'ESTADO',
                    'Nombre de la parcela': 'NOMBRE_DE_LA_PARCELA',
                    'Superficie trabajada': 'SUPERFICIE_TRABAJADA',
                    'Tipo de': 'TIPO_DE',
                    'Volumen de caldo (hl)': 'VOLUMEN_DE_CALDO_HL',
                    'Tipo de familia': 'TIPO_DE_FAMILIA',
                    'Nombre específico': 'NOMBRE_ESPECÍFICO',
                    'Cantidad': 'CANTIDAD',
                    'Unidad': 'UNIDAD',
                    'Coste (€)': 'COSTE_€',
                    'Materia activa': 'MATERIA_ACTIVA'}

prioridadList=["Fecha inicio",
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
GeofoliaTables=['geo_cosecha', 
'geo_cultivo', 
'geo_implantacion', 
'geo_mano_obra', 
'geo_maquinaria', 
'geo_materia_prima', 
'geo_tipos_de_suelo', 
'geo_variedad',
'geo_parcela',
'geo_partes']


engine=SQLengine()
oldTablesDict={}
# newTablesDict["geo_cosecha"].to_sql("geo_cosecha",engine,if_exists="replace",index=False,method="multi")


for i in GeofoliaTables:
    print(f"------------------------------------- {i} -------------------------------------------")
    print("-------------------------------------OLD-------------------------------------------")
    t0=datetime.datetime.now()
    oldTablesDict[i]=pd.read_sql_table(i, engine)
    timeDict[i].append(datetime.datetime.now()-t0)
    print(oldTablesDict[i])
    print("-------------------------------------NEW-------------------------------------------")
    print(newTablesDict[i])
    print(oldTablesDict[i].equals(newTablesDict[i]))

for i in GeofoliaTables:
    print(f"------------------------------------- {i} -------------------------------------------")
    t0=datetime.datetime.now()
    if i!= "geo_partes":
        newTablesDict[i].to_sql(i,engine,if_exists="replace",index=False,method="multi")
    else:
        chunk=int(round(len(newTablesDict[i])/500,0))
        newTablesDict[i].to_sql(i,engine,if_exists="replace",index=False,chunksize=chunk,method="multi")
    print(datetime.datetime.now()-t0)
    timeDict[i].append(datetime.datetime.now()-t0)

completeCOmpelationTime=datetime.datetime.now()-AT0
print("-----------------------------Compilation Time:----------------------\n",completeCOmpelationTime)
ReplaceTime=pd.DataFrame(timeDict,index=["Normalizing Time", "Old Table SQL TIME","SQL Query Time"])

ReplaceTime.T.to_excel("ManipulationTime.xlsx")
