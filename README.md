## SampleDataInputFormat

You want to create sample (dummy) data on the fly in Hadoop by specifying some simple rules? Yeah, so did I!

SampleDataInputFormat generates sample records, which are then passed to the map() calls of your MapReduce job. The key has no content, and the value contains an ASCII-1 separated string of the sample values.

Each field can be generated by one of three methods:

* Range: Specify a start and end value and SampleDataInputFormat will pick a random value in the range.

* Enum: Specify a list of values and SampleDataInputFormat will pick a random value from the list. Use a single entry list to define a static value for the field.

* UUID: SampleDataInputFormat will use the Java UUID library to generate a random 128-bit value. This can be used for unique key fields.

You also specify the chance that each field value will be NULL.

## Usage

### Properties
The rules for SampleDataInputFormat can be passed either by MapReduce job properties or by Hive TBLPROPERTIES.

<table>
  <tr>
    <th>Property</th><th>Definition</th><th>Domain</th>
  </tr>
  <tr>
    <td>sampledata.mappers</td><td>Number of mappers (Hive forces 1)</td><td>&gt;=1</td>
  </tr>
  <tr>
    <td>sampledata.records</td><td>Number of records across all mappers</td><td>&gt;=1</td>
  </tr>
  <tr>
    <td>sampledata.fieldnames</td><td>Field names to use for later properties</td><td>Comma-separated list</td>
  </tr>
  <tr>
    <td>sampledata.fields.{fieldname}.type</td><td>Data type</td><td>"string", "int", "double" or "date"</td>
  </tr>
  <tr>
    <td>sampledata.fields.{fieldname}.date.format</td><td>Format of date strings</td><td>As per java.text.SimpleDateFormat, e.g. "yyyy/MM/dd"</td>
  </tr>
  <tr>
    <td>sampledata.fields.{fieldname}.nulls.weight</td><td>Chance that a value will be NULL</td><td>0.0 to 1.0</td>
  </tr>
  <tr>
    <td>sampledata.fields.{fieldname}.method</td><td>How to generate this field</td><td>"range", "enum" or "uuid"</td>
  </tr>
  <tr>
    <td>sampledata.fields.{fieldname}.range.start</td><td>Lower-bound of range. Not valid for string.</td><td>Inclusive</td>
  </tr>
  <tr>
    <td>sampledata.fields.{fieldname}.range.end</td><td>Upper-bound of range. Not valid for string.</td><td>Exclusive</td>
  </tr>
  <tr>
    <td>sampledata.fields.{fieldname}.enum.values</td><td>List of enum values</td><td>Comma-separated list</td>
  </tr>
</table>

Caveat emptor - there is nearly no error checking!

### Hive

The easiest way to use SampleDataInputFormat is through Hive. An external table is created that points to a real HDFS directory, however no data is read from it.

The data generation rules are specified in the Hive table DDL's TBLPROPERTIES. Each SELECT from the table will dynamically bring back a new set of sample records.

The Hive DDL syntax can be gleaned from the [example DDL script](http://github.mtv.cloudera.com/jeremy/SampleDataInputFormat/blob/master/src/scripts/createtable.sql).


It is important when using Hive that you force the query to use MapReduce. If you run a SELECT * with no filters or joins, such as ``SELECT * FROM table LIMIT 100;`` Hive will skip MapReduce and just use the InputFormat to return the rows to screen, but the rules will not be passed to it. You can force MapReduce by adding a tautology filter, such as ``SELECT * FROM table WHERE 1=1 LIMIT 100;``


You can override the parameters from a script or within the Hive shell, for example: ``SET sampledata.records=100000;``


Note that due to some poor design decisions in Hive it will require extra code to SampleDataInputFormat to enable Hive to use multiple mappers. Until that is added Hive will only run this with a single mapper.

### MapReduce

I have not tested this with plain MapReduce, but it should work fine by passing all the parameters to the job.