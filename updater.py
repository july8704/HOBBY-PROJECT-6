import configparser
import pyodbc
import sys
import math
import pandas as pd

# get the SQLSERVER connection info and connect
parser = configparser.ConfigParser()
parser.read("credential.conf")
server = parser.get ("SQLSERVER_config", "server")
username = parser.get("SQLSERVER_config","username")
dbname = parser.get("SQLSERVER_config","dbname")
password = parser.get("SQLSERVER_config","password")

conn = pyodbc.connect('Driver={ODBC Driver 18 for SQL Server};'
                      'Server='+ server +';'
                      'Database='+ dbname +';'
                      'UID='+ username +';'
                      'PWD='+ password +';'
                      'TrustServerCertificate=yes;')

productos = pd.read_fwf("Source/Productos.txt", widths = [12,51,51,9])

cursor = conn.cursor()
start = 0
stop = 1000
particiones = math.ceil( len(productos.axes[0])/1000)
while particiones > 0 :
    productos_parted = productos[start:stop]
    productos_parted.Name = productos_parted.Name.str.replace("'","\'\'")
    productos_parted.ProductModel = productos_parted.ProductModel.str.replace("'", "\'\'")
    tuples = [tuple(x) for x in productos_parted.to_numpy()]
    cols = ','.join(list(productos_parted.columns))
    values = ','.join(map(str, tuples))
    values = values.replace('"','\'')

    # SQL query to execute
    SQL_drop_temp = "DROP TABLE IF EXISTS ##tmp_Productos"
    SQL_ansi = "SET ANSI_NULLS ON"
    SQL_quoted  = "QUOTED_IDENTIFIER ON"
    SQL_create_temp = '''
        CREATE TABLE ##tmp_Productos (
       [ProductID] [int] NOT NULL,
       [Name] [nvarchar](52) NULL,
        [ProductModel] [nvarchar](52) NULL,
        [CultureID] [nvarchar](9) NOT NULL
    ) ON [PRIMARY]
    '''
    SQL_primaryKey = '''
        ALTER TABLE ##tmp_Productos ADD PRIMARY KEY CLUSTERED 
        (
           [ProductID], [CultureID]
        )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
        '''
    SQL_insert = "insert into ##tmp_Productos ([ProductID],[Name],[ProductModel],[CultureID]) VALUES {}".format(values)

    SQL_MERGE = '''
    MERGE INTO dbo.Productos AS pr
USING (SELECT * FROM ##tmp_Productos) AS temp 
    ON pr.ProductID = temp.ProductID and pr.CultureID = temp.CultureID
WHEN MATCHED THEN
    UPDATE SET pr.Name = temp.Name, pr.ProductModel=temp.ProductModel
WHEN NOT MATCHED BY TARGET THEN
    INSERT ([ProductID] ,
	[Name],
	[ProductModel],
	[CultureID])
    VALUES (temp.ProductID,temp.Name,temp.ProductModel,temp.CultureID);
    '''
 


    cursor.execute(SQL_drop_temp)
    cursor.execute(SQL_create_temp)
    cursor.execute(SQL_primaryKey)
    cursor.execute(SQL_insert)
    cursor.execute(SQL_MERGE)
    cursor.commit()
    print (particiones)
    particiones = particiones -1
    start = stop
    stop += 1000
cursor.close()
