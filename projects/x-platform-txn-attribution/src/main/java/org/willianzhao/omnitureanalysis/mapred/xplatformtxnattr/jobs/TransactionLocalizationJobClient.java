package org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

/**
 * Created by weilzhao on 8/21/14.
 */
public class TransactionLocalizationJobClient extends Configured implements Tool {
    Configuration config;

    @Override
    public int run(String[] strings) throws Exception {
        return 0;
    }
}
