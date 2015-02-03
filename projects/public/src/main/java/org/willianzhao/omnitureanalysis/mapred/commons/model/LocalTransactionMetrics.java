package org.willianzhao.omnitureanalysis.mapred.commons.model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by weilzhao on 8/21/14.
 */
public class LocalTransactionMetrics  implements WritableComparable {

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
    }
}
