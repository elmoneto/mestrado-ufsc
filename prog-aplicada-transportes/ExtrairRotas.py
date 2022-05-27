
#===================================================IMPORTS
from distutils.log import error
from OSMPythonTools.overpass import Overpass, overpassQueryBuilder
from OSMPythonTools.nominatim import Nominatim
from sqlalchemy import create_engine
import psycopg2
import json

#==================================================CONF BANCO
user='postgres'
password='jackiechan'
host='localhost'
dbname='trabdemo2'
port='5432'
connection_string = f"dbname='{dbname}' user='{user}' host='{host}' password='{password}'"
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')

conn = psycopg2.connect(connection_string)
conn.autocommit = True
cur = conn.cursor()


overpass = Overpass()
nominatim = Nominatim()
areaID =  nominatim.query('Florianópolis, Brazil').areaId()

query = overpassQueryBuilder(area=areaID,elementType='relation',selector='"route"="bus"', out='body')
print(query)
rotas = overpass.query(query)
rotas_json = rotas.toJSON()

# parse file
#rotas_json = json.loads(dados)

cont_rota = 0
cont_segmento = 0
rotas_vias = []
rotas_paradas = []

for rota in rotas_json['elements']:
    id_rota = rota['id']
    sequencia_paradas = 0
    sequencia_segmentos = 0
    for membro in rota['members']:
        if(membro['role'] == 'platform'):
            rotas_paradas.append([id_rota,sequencia_paradas,membro['ref']])
            sequencia_paradas+=1
        if(membro['role'] == ''):
            rotas_vias.append([id_rota,sequencia_segmentos,membro['ref']])
            sequencia_segmentos+=1

with conn:
    cur.execute("DROP TABLE IF EXISTS rotas_vias")
    cur.execute("DROP TABLE IF EXISTS rotas_paradas")
    cur.execute("COMMIT")
    try:
        cur.execute("CREATE TABLE rotas_vias(id serial primary key, osmid_rota varchar(25), sequencia integer, osmid_via varchar(25))")
        cur.execute("CREATE TABLE rotas_paradas(id serial primary key, osmid_rota varchar(25), sequencia integer, osmid_parada varchar(25))")
        cur.execute("COMMIT")
    except(error):
        print(error)

    for rota_via in rotas_vias:

        insert_string = f"INSERT INTO rotas_vias(osmid_rota, sequencia, osmid_via) VALUES ('{rota_via[0]}', {rota_via[1]}, '{rota_via[2]}')"
        cur.execute(insert_string)
        print(insert_string)
    cur.execute("COMMIT")

    for rota_parada in rotas_paradas:
        insert_string = f"INSERT INTO rotas_paradas(osmid_rota, sequencia, osmid_parada) VALUES ('{rota_parada[0]}', {rota_parada[1]}, '{rota_parada[2]}')"
        cur.execute(insert_string)
        print(insert_string)
    cur.execute("COMMIT")
