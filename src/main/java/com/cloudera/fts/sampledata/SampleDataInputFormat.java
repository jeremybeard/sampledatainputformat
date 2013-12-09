package com.cloudera.fts.sampledata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

// use the old mapred API so we're compatible with Hive. sorry, Pig.
public class SampleDataInputFormat implements InputFormat<NullWritable, Text> {

    public RecordReader<NullWritable, Text> getRecordReader(InputSplit ignoredSplit,
            JobConf job, Reporter reporter) throws IOException {
        return new SampleDataRecordReader(job);
    }

    // we can run multiple mappers if called directly from MR, but Hive gets all tricky
    // on us and incomprehendingly assumes the InputFormat is based on FileInputFormat,
    // which indirectly blocks us starting more than one mapper without a whole bunch of
    // extra code being added. maybe later.
    public InputSplit[] getSplits(JobConf job, int suggestedSplits) throws IOException {        
        int numSplits = Integer.parseInt(job.get("sampledata.mappers", "1"));
        InputSplit[] splits = new EmptySplit[numSplits];

        for (int i = 0; i < numSplits; i++)
            splits[i] = new EmptySplit();

        return splits;
    }
    
    // the splits are used purely to run the job in parallel. there's not actually any
    // stored data that we are trying to split up, so we fake it until we make it
    public static class EmptySplit implements InputSplit, Writable {
        public void write(DataOutput out) throws IOException { }
        public void readFields(DataInput in) throws IOException { }
        public long getLength() {
            return 0L;
        }
        public String[] getLocations() {
            return new String[0];
        }
    }
}