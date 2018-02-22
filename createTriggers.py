#!/usr/bin/python
import sys
import psycopg2
from lxml import etree

operation=sys.argv[4]
conn_string=sys.argv[3] # "host='localhost' dbname='my_database' user='postgres' password='secret'"
schema=sys.argv[2]
ns={'ns0':'http://www.deegree.org/datasource/feature/sql'}

def createChangelog():
  return """CREATE TABLE IF NOT EXISTS %s.endringslogg
(
  tabell character varying,
  type character(1),
  tidspunkt timestamp with time zone,
  lokalid character varying,
  endringsid bigserial NOT NULL,
  CONSTRAINT endringslogg_pk PRIMARY KEY (endringsid)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE %s.endringslogg
  OWNER TO postgres;""" % (schema, schema)

def createTableTrigger(name, table):
  return """CREATE TRIGGER %s_endringslogg
  AFTER INSERT OR UPDATE OR DELETE
  ON %s.%s
  FOR EACH ROW
  EXECUTE PROCEDURE %s.endringslogg_func('%s');""" % (table, schema, table, schema, name)

def dropTableTrigger(name, table):
  return """DROP TRIGGER IF EXISTS %s_endringslogg ON %s.%s""" % (table, schema, table)

def createTrigger():
  return """CREATE OR REPLACE FUNCTION %s.endringslogg_func()
  RETURNS trigger AS
$BODY$
BEGIN
  IF (TG_OP = 'DELETE') THEN
    INSERT INTO %s.endringslogg 
    SELECT 
    TG_ARGV[0],
    'D', 
    now(), 
    OLD.lokalid;
    RETURN OLD;
  ELSIF (TG_OP = 'UPDATE') THEN
    INSERT INTO %s.endringslogg 
		SELECT 
      TG_ARGV[0],
			'U', 
			now(), 
			NEW.lokalid;
    RETURN NEW;
  ELSIF (TG_OP = 'INSERT') THEN
    INSERT INTO %s.endringslogg 
		SELECT 
      TG_ARGV[0],
			'I', 
			now(), 
			NEW.lokalid;
		RETURN NEW;
	END IF;
  RETURN NULL; -- result is ignored since this is an AFTER trigger
END;$BODY$
LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION %s.endringslogg_func()
  OWNER TO postgres;""" % (schema, schema, schema, schema, schema)

def executeSql(sql):
  print "Connecting to database\n	->%s" % (conn_string)
  conn = psycopg2.connect(conn_string)
  cursor = conn.cursor()
  cursor.execute(sql)

def createAll():
  print("Creating change-table")
  executeSql(createChangelog())
  print("Creating trigger-function")
  executeSql(createTrigger())

def dropTriggers():
  for nameTable in getMappings():
    executeSql(dropTableTrigger(nameTable[0], nameTable[1]))
  
def createTriggers():
  for nameTable in getMappings():
    executeSql(createTableTrigger(nameTable[0], nameTable[1]))

def getMappings():
  mappings=[]
  for featureType in tree.xpath('//ns0:FeatureTypeMapping', namespaces=ns):
    name=featureType.get('name').split(':')[1]
    table=featureType.get('table')
    print("Creating trigger for featureType " + name)
    mappings.append([ name, table ])
  return mappings
    

tree = etree.parse(open(sys.argv[1])) # read deegree featureStore

if(operation == 'init'):
  createAll()
elif(operation == 'stop'):
  dropTriggers()
elif(operation == 'start'):
  createTriggers()
else:
  print("Valid operations are init, stop, start")

