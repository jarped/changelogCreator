#!/usr/bin/python
import sys
import psycopg2
from lxml import etree

conn_string=sys.argv[3] # "host='localhost' dbname='my_database' user='postgres' password='secret'"
schema=sys.argv[2]
ns={'ns0':'http://www.deegree.org/datasource/feature/sql'}

def createChangelog():
  return """CREATE TABLE """ + schema + """.endringslogg
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
ALTER TABLE """ + schema + """.endringslogg
  OWNER TO postgres;"""

def createTableTrigger(name, table):
  return """CREATE TRIGGER """ + table + """_endringslogg
  AFTER INSERT OR UPDATE OR DELETE
  ON """ + schema + """.""" + table + """
  FOR EACH ROW
  EXECUTE PROCEDURE """ + schema + """.endringslogg_func('""" + name + """');"""

def createTrigger():
  return """CREATE OR REPLACE FUNCTION """ + schema + """.endringslogg_func()
  RETURNS trigger AS
$BODY$
BEGIN
  IF (TG_OP = 'DELETE') THEN
    INSERT INTO """ + schema + """.endringslogg 
    SELECT 
    TG_ARGV[0],
    'D', 
    now(), 
    OLD.lokalid;
    RETURN OLD;
  ELSIF (TG_OP = 'UPDATE') THEN
    INSERT INTO """ + schema + """.endringslogg 
		SELECT 
      TG_ARGV[0],
			'U', 
			now(), 
			NEW.lokalid;
    RETURN NEW;
  ELSIF (TG_OP = 'INSERT') THEN
    INSERT INTO """ + schema + """.endringslogg 
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
ALTER FUNCTION """ + schema + """.endringslogg_func()
  OWNER TO postgres;"""

def executeSql(sql):
  print "Connecting to database\n	->%s" % (conn_string)
  conn = psycopg2.connect(conn_string)
  cursor = conn.cursor()
  cursor.execute(sql)

tree = etree.parse(open(sys.argv[1])) # read deegree featureStore

print("Creating change-table")
executeSql(createChangelog())
print("Creating trigger-function")
executeSql(createTrigger())

for featureType in tree.xpath('//ns0:FeatureTypeMapping', namespaces=ns):
  name=featureType.get('name').split(':')[1]
  table=featureType.get('table')
  print("Creating trigger for featureType " + name)
  executeSql(createTableTrigger(name, table))