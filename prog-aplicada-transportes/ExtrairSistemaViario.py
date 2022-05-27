#=========================IMPORTS
import json
from OSMPythonTools.overpass import Overpass, overpassQueryBuilder
from OSMPythonTools.nominatim import Nominatim
from sqlalchemy import create_engine
import psycopg2

#========================CONF BANCO
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

query = overpassQueryBuilder(area=areaID,elementType='way',selector='"highway"~"motorway|motorway_link|trunk|trunk_link|primary|primary_link|secondary|secondary_link|tertiary|tertiary_link|residential|unclassified"', out='geom')

sistema_viario = overpass.query(query)
sistema_viario_json = sistema_viario.toJSON()

with open('sistema_viario.json', 'w', encoding='utf-8') as f:
    json.dump(sistema_viario_json, f, ensure_ascii=False, indent=3)

# ruas_explodidas = [['highway','osm_id','name','surface','max_speed','one_way','geom']]
ruas_explodidas = []

print("Processando os dados obtidos do OpenStreetMap...")
for way in sistema_viario_json['elements']:
    highway = way['tags']['highway']
    osm_id = way['id']

    if('name' in way['tags']):
        name = way['tags']['name']
    else:
        name = ''

    if('surface' in way['tags']):
        surface = way['tags']['surface']
    else:
        surface = ''

    if('maxspeed' in way['tags']):
        maxspeed = way['tags']['maxspeed']
    else:
        maxspeed = ''

    if('oneway' in way['tags']):
        oneway = way['tags']['oneway']
    else:
        oneway = ''

    geometria = []

    for geometry in way['geometry']:
        geometria.append( (geometry['lon'], geometry['lat']) )

    for i in range(len(geometria)-1):
        linha = f'LINESTRING({geometria[i][0]} {geometria[i][1]}, {geometria[i+1][0]} {geometria[i+1][1]})'
        ruas_explodidas.append([highway,osm_id,name,surface,maxspeed,oneway,linha])

print("Criando infra do banco...")
erros = []
cont_erros = 0
with conn:
    cur.execute("DROP TABLE IF EXISTS vias")
    cur.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    cur.execute("CREATE EXTENSION IF NOT EXISTS pgrouting")
    cur.execute("COMMIT")

    cur.execute("CREATE TABLE vias(id serial primary key,highway varchar(25),osmid varchar(25),name varchar(50),surface varchar(25),maxspeed varchar(25),oneway varchar(25))")
    cur.execute("COMMIT")

    cur.execute("SELECT AddGeometryColumn ('public','vias','geom',4326,'LINESTRING',2)")
    cur.execute("SELECT AddGeometryColumn ('public','vias','geom_utm22s',31982,'LINESTRING',2)")
    cur.execute("COMMIT")

    for rua in ruas_explodidas:
        insert_string = f"INSERT INTO vias(highway,osmid,name,surface,maxspeed,oneway,geom) VALUES ( '{rua[0]}', '{rua[1]}', '{rua[2]}',  '{rua[3]}', '{rua[4]}', '{rua[5]}', ST_GeomFromText('{rua[6]}',4326))"
        try:
            cur.execute(insert_string)
        except:
            cont_erros+=1
            erros.append(insert_string)
        cur.execute("COMMIT")
    
    cur.execute("UPDATE vias SET geom_utm22s = ST_Transform(geom,31982)")
    cur.execute("COMMIT")
    cur.execute("ALTER TABLE vias ADD COLUMN volume integer")
    cur.execute("ALTER TABLE vias ADD COLUMN cost real")
    cur.execute("ALTER TABLE vias ADD COLUMN reverse_cost real")
    cur.execute("ALTER TABLE vias ADD COLUMN source integer")
    cur.execute("ALTER TABLE vias ADD COLUMN target integer")
    cur.execute("COMMIT")

    cur.execute("CREATE INDEX vias_source_idx ON vias (source)")
    cur.execute("COMMIT")
    cur.execute("CREATE INDEX vias_target_idx ON vias (target)")
    cur.execute("COMMIT")

    cur.execute("UPDATE vias SET cost = ST_Length(geom_utm22s) / 1000")
    cur.execute("COMMIT")

    cur.execute("UPDATE vias SET reverse_cost = ST_Length(geom_utm22s) / 1000")
    cur.execute("COMMIT")

    cur.execute("UPDATE vias SET reverse_cost = -1 WHERE oneway = 'yes'")
    cur.execute("COMMIT")

    cur.execute("CREATE INDEX vias_idx ON vias (id)")
    cur.execute("COMMIT")

    print("Criando topologia da malha viária...")
    cur.execute("SELECT pgr_createTopology('vias', 0.01, 'geom_utm22s','id')")
    cur.execute("COMMIT")


