package com.cloudera.fts.sampledata;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

public class SampleDataRecordReader implements RecordReader<NullWritable, Text> {
    
    JobConf jobConf;
    String name;
    int numRecords;
    List<SampleField> fields;
    int recordsReturned;
    
    // load up the rules for deriving sample records
    SampleDataRecordReader(JobConf job) {
        jobConf = job;
        name = getProperty("sampledata.name");
        numRecords = Integer.parseInt(getProperty("sampledata.records"));
        fields = new ArrayList<SampleField>();
        
        String[] fieldNames = getProperty("sampledata.fieldnames").split(",");
        
        // load the rules for each field one at a time
        for (String fieldName : fieldNames) {
            SampleField field = new SampleField();
            // valid types: string, int, double, date
            field.type = getProperty("sampledata.fields." + fieldName + ".type");
            // chance that the field should be NULL, between 0.0 and 1.0 inclusive
            field.nullWeight = Double.parseDouble(getProperty("sampledata.fields." + fieldName + ".nulls.weight"));
            // valid methods: range, enum, uuid
            field.method = getProperty("sampledata.fields." + fieldName + ".method");
            
            if (field.type.equals("date")) {
                field.dateFormat = getProperty("sampledata.fields." + fieldName + ".date.format");
            }
            
            if (field.method.equals("range")) {
                field.rangeStart = getProperty("sampledata.fields." + fieldName + ".range.start");
                field.rangeEnd = getProperty("sampledata.fields." + fieldName + ".range.end");
            }
            else if (field.method.equals("enum")) {
                field.enumValues = getProperty("sampledata.fields." + fieldName + ".enum.values").split(",");
            }
            
            fields.add(field);
        }
        
        recordsReturned = 0;
    }
    
    public void close() throws IOException {        
    }

    public NullWritable createKey() {
        // dat singleton
        return NullWritable.get();
    }

    public Text createValue() {
        return new Text();
    }

    public long getPos() throws IOException {
        return recordsReturned / (numRecords / jobConf.getNumMapTasks());
    }

    public float getProgress() throws IOException {
        return 0;
    }

    // build a sample data record using the provided rules
    public boolean next(NullWritable key, Text value) throws IOException {
        String record = "";
        // provide an ASCII-1 delimited text record to the mapper
        String separator = Character.valueOf((char) 1).toString();
        Random random = new Random();
        
        // build the sample record one field at a time...
        for (SampleField field : fields) {
            // create a value for the field if we don't decide it should be NULL
            if (random.nextDouble() > field.nullWeight) {
                // use the field method to find out how we should create the value
                
                // ranges are a random value >= start and < end
                if (field.method.equals("range")) {
                    if (field.type.equals("int")) {
                        int range = Integer.parseInt(field.rangeEnd) - Integer.parseInt(field.rangeStart);
                        int randInRange = random.nextInt(range) + Integer.parseInt(field.rangeStart);
                        
                        record += String.valueOf(randInRange);
                    }
                    else if (field.type.equals("double")) {
                        double range = Double.parseDouble(field.rangeEnd) - Double.parseDouble(field.rangeStart);
                        double randInRange = random.nextDouble() * range + Double.parseDouble(field.rangeStart);
                        
                        record += String.valueOf(randInRange);
                    }
                    else if (field.type.equals("date")) {
                        try {
                            // find a random date by converting to UNIX time then converting back
                            SimpleDateFormat format = new SimpleDateFormat(field.dateFormat);
                            java.util.Date startDate = format.parse(field.rangeStart);
                            java.util.Date endDate = format.parse(field.rangeEnd);
                            long range = endDate.getTime() - startDate.getTime();
                            long randInRange = (long)(random.nextDouble() * range) + startDate.getTime();
                            java.util.Date randDate = new java.util.Date(randInRange);
                            
                            record += format.format(randDate);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                    }
                }
                // enums are a random value from a pre-defined list
                else if (field.method.equals("enum")) {
                    int enumSize = field.enumValues.length;
                    String randInEnum = field.enumValues[random.nextInt(enumSize)];
                    
                    record += randInEnum;
                }
                // uuids are (extremely nearly) always unique. great for keys!
                else if (field.method.equals("uuid")) {
                    record += UUID.randomUUID().toString();
                }
            }
            // we decided this value should be NULL
            else {
                // INTs and DOUBLEs are left blank, but Hive expects a \N
                // to represent NULL
                if (field.type.equals("string"))
                    record += "\\N";
            }
            
            // add the separator to the end of each value
            record += separator;
        }
        
        // make sure we don't finish the record with a separator (that's silly)
        if (record.endsWith(separator))
            record = record.substring(0, record.length() - separator.length());
        
        // attach the record to the provided Text value object
        value.set(record);
        
        // let MR know if we are done processing records
        return ++recordsReturned <= numRecords / jobConf.getNumMapTasks();
    }
    
    // get the runtime property either from MR or Hive
    private String getProperty(String property) {
        String jobProp = jobConf.get(property);
        
        // Look for the property in MR first
        if (jobProp != null) {
            return jobProp;
        // If not found, look for the property in the Hive TBLPROPERTIES
        } else {
            MapredWork mapRedWork = Utilities.getMapRedWork(jobConf);
            for (Map.Entry<String,PartitionDesc> pathsAndParts: mapRedWork.getPathToPartitionInfo().entrySet()) {
                Properties props = pathsAndParts.getValue().getProperties();
                return props.getProperty(property);
            }
            
            return null;
        }
    }

}
