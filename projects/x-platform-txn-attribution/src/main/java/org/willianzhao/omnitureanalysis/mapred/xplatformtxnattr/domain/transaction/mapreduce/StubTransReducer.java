package org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transaction.mapreduce;

import org.willianzhao.omnitureanalysis.mapred.commons.model.VisitStep;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by willianzhao on 6/11/14.
 */
public class StubTransReducer extends Reducer<Text, VisitStep, NullWritable, VisitStep> {
    @Override
    protected void reduce(Text key, Iterable<VisitStep> values,
                          Context context) throws IOException, InterruptedException {
        Iterator<VisitStep> it = values.iterator();
        VisitStep uniqueTrans = it.next();
        NullWritable dummyKey = NullWritable.get();
        context.write(dummyKey, uniqueTrans);
    }
}
