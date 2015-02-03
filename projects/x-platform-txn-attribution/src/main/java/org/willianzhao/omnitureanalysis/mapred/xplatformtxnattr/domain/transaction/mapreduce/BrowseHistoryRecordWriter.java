package org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transaction.mapreduce;

import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by willianzhao on 5/13/14.
 */
public class BrowseHistoryRecordWriter extends RecordWriter<NullWritable, Text> {

    private DataOutputStream out;

    public BrowseHistoryRecordWriter(DataOutputStream out) {
        this.out = out;
    }

    @Override
    public void write(NullWritable key, Text outputValue) throws IOException, InterruptedException {
        String formatted = outputValue.toString() + ProjectConstant.CONSTANT_NEWLINE;
        out.write(formatted.getBytes(), 0, formatted.length());
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        out.close();
    }
}
