package org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transByTimezone.geoParse.mapreduce;

import org.willianzhao.omnitureanalysis.mapred.commons.model.TransactionGeography;
import org.willianzhao.omnitureanalysis.mapred.commons.model.TransactionRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by weilzhao on 8/21/14.
 */
public class GeoParseReducer extends Reducer<Text,TransactionGeography,NullWritable,TransactionGeography> {
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text key, Iterable<TransactionGeography> values, Context context) throws IOException, InterruptedException {
        Iterator<TransactionGeography> trItr = values.iterator();
        if (trItr.hasNext()) {
            TransactionGeography transactionGeography=trItr.next();
            NullWritable nullWritable=NullWritable.get();
            context.write(nullWritable,transactionGeography);
        }
    }
}
