package org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.willianzhao.omnitureanalysis.mapred.commons.hdfs.StubHubHDFSReader;
import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;
import org.willianzhao.omnitureanalysis.mapred.commons.model.VisitStep;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transaction.mapreduce.StubTransMapper;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transaction.mapreduce.StubTransReducer;

/**
 * Created by willianzhao on 6/10/14.
 */
public class StubTransClient extends Configured implements Tool {

    Configuration config;
	private static Logger logger = LoggerFactory.getLogger(StubTransClient.class);
    
    @Override
    public int run(String[] strings) throws Exception {
        config = getConf();
        StubHubHDFSReader pathReader = new StubHubHDFSReader(config);
        String jobName = config.get(ProjectConstant.PARAM_LABEL_JOBNAME_RUNTIME);
        String transactionDateStr = config.get(ProjectConstant.PARAM_LABEL_TRANSDATE_RUNTIME);
        String stubtrans_inputpath = null;
        String feed_data_outputpath = null;
        stubtrans_inputpath = pathReader.getHDFSInputPathForStubTrans(transactionDateStr);
        feed_data_outputpath = pathReader.getHDFSOutputPathForStubTrans(transactionDateStr,config.get(ProjectConstant.PARAM_LABEL_MODE));
        if (pathReader.isPathNonEmpty(feed_data_outputpath)) {
            logger.info("The stub_trans feed data on {} is already existed and not empty", feed_data_outputpath);
            return 0;
        }
        pathReader.removePathIfExisted(feed_data_outputpath);
        Job job = Job.getInstance(config, jobName);
        job.setMapperClass(StubTransMapper.class);
        job.setReducerClass(StubTransReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VisitStep.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(VisitStep.class);
        FileInputFormat.addInputPaths(job, stubtrans_inputpath);
        job.setInputFormatClass(TextInputFormat.class);
        Path dir = new Path(feed_data_outputpath);
        FileOutputFormat.setOutputPath(job, dir);
        job.setJarByClass(StubTransClient.class);
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;

    }
}
