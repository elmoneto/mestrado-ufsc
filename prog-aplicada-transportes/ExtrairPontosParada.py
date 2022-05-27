from distutils.log import error
import json
from OSMPythonTools.overpass import Overpass, overpassQueryBuilder
from OSMPythonTools.nominatim import Nominatim
from sqlalchemy import create_engine
import psycopg2

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

query = overpassQueryBuilder(area=areaID,elementType='node',selector='"public_transport"~"platform"', out='geom')

pontos_parada = overpass.query(query)
pontos_parada_json = pontos_parada.toJSON()

with open('pontos_parada.json', 'w', encoding='utf-8') as f:
    json.dump(pontos_parada_json, f, ensure_ascii=False, indent=3)

lista_pontos = []
#[osmid,shelter,geom]

for node in pontos_parada_json['elements']:
    osmid = node['id']
    if 'shelter' in node['tags']:
        shelter = 'yes'
    else:
        shelter = ''

    if 'bench' in node['tags']:
        bench = 'yes'
    else:
        bench = 'no'

    geom = f"POINT({node['lon']} {node['lat']})"

    lista_pontos.append([osmid,shelter,bench,geom])

with conn:
    cur.execute("DROP TABLE IF EXISTS paradas")
    cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    cur.execute("CREATE EXTENSION IF NOT EXISTS pgrouting")
    cur.execute("COMMIT")

    cur.execute("CREATE TABLE paradas(id serial primary key,osmid varchar(25), shelter varchar(25), bench varchar(50))")
    cur.execute("COMMIT")

    cur.execute("SELECT AddGeometryColumn ('public','paradas','geom',4326,'POINT',2)")
    cur.execute("COMMIT")

    for ponto in lista_pontos:
        insert_string = f"INSERT INTO paradas(osmid, shelter, bench ,geom) VALUES ( '{ponto[0]}', '{ponto[1]}', '{ponto[2]}', ST_GeomFromText('{ponto[3]}',4326))"
        try:
            cur.execute(insert_string)
        except(error):
            print(error)
            # cont_erros+=1
            # erros.append(insert_string)
        cur.execute("COMMIT")


