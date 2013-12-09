ADD JAR sampledata-0.0.1-SNAPSHOT.jar;

DROP TABLE IF EXISTS sampledata;

CREATE EXTERNAL TABLE sampledata
(
    key STRING
  , a INT
  , b DOUBLE
  , c STRING
  , d STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
STORED AS
INPUTFORMAT 'com.cloudera.fts.sampledata.SampleDataInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/user/cloudera/sampledata'
TBLPROPERTIES
(
    'sampledata.name' = 'sampledata'
  , 'sampledata.records' = '20'
  , 'sampledata.mappers' = '3'
  , 'sampledata.fieldnames' = 'key,a,b,c,d'
  , 'sampledata.fields.key.type' = 'string'
  , 'sampledata.fields.key.nulls.weight' = '0'
  , 'sampledata.fields.key.method' = 'uuid'
  , 'sampledata.fields.a.type' = 'int'
  , 'sampledata.fields.a.nulls.weight' = '0'
  , 'sampledata.fields.a.method' = 'range'
  , 'sampledata.fields.a.range.start' = '100'
  , 'sampledata.fields.a.range.end' = '110'
  , 'sampledata.fields.b.type' = 'double'
  , 'sampledata.fields.b.nulls.weight' = '0.5'
  , 'sampledata.fields.b.method' = 'range'
  , 'sampledata.fields.b.range.start' = '120.1'
  , 'sampledata.fields.b.range.end' = '120.2'
  , 'sampledata.fields.c.type' = 'string'
  , 'sampledata.fields.c.nulls.weight' = '0.9'
  , 'sampledata.fields.c.method' = 'enum'
  , 'sampledata.fields.c.enum.values' = 'hello,world,herp,derp'
  , 'sampledata.fields.d.type' = 'date'
  , 'sampledata.fields.d.date.format' = 'yyyy/MM/dd'
  , 'sampledata.fields.d.nulls.weight' = '0.05'
  , 'sampledata.fields.d.method' = 'range'
  , 'sampledata.fields.d.range.start' = '2013/11/01'
  , 'sampledata.fields.d.range.end' = '2014/01/01'
)
;
