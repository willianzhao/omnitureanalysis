package org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.jobs;

import org.willianzhao.omnitureanalysis.mapred.commons.hdfs.StubHubHDFSReader;
import org.willianzhao.omnitureanalysis.mapred.commons.misc.ConfigureReader;
import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;
import org.willianzhao.omnitureanalysis.mapred.commons.model.TransactionRecord;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transByTimezone.record.mapreduce.TransRecordMapper;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transByTimezone.record.mapreduce.TransRecordReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by weilzhao on 8/21/14.
 */
public class TransRecordJobClient extends Configured implements Tool {
    Configuration config;
    HashMap<String, Integer> colsMap = null;
	private static Logger logger = LoggerFactory.getLogger(TransRecordJobClient.class);

    @Override
    public int run(String[] strings) throws Exception {
        config = getConf();
        initialColPosition();
        StubHubHDFSReader pathReader = new StubHubHDFSReader(config);
        String jobName = config.get(ProjectConstant.PARAM_LABEL_JOBNAME_RUNTIME);
        String transactionDateStr = config.get(ProjectConstant.PARAM_LABEL_TRANSDATE_RUNTIME);
        //Get input and output Path
        String transRec_inputpath = null;
        String transRec_outputpath = null;
        transRec_inputpath = pathReader.getHDFSInputPathForTransRecord(transactionDateStr);
        transRec_outputpath = pathReader.getHDFSOutputPathForTransjobs(ProjectConstant.PATH_ALL_TRANS_RECORD, transactionDateStr);
        pathReader.removePathIfExisted(transRec_outputpath);
        Job job = Job.getInstance(config, jobName);
        job.setMapperClass(TransRecordMapper.class);
        job.setReducerClass(TransRecordReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TransactionRecord.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(TransactionRecord.class);
        FileInputFormat.addInputPaths(job, transRec_inputpath);
        job.setInputFormatClass(TextInputFormat.class);
        Path dir = new Path(transRec_outputpath);
        FileOutputFormat.setOutputPath(job, dir);
        job.setJarByClass(TransRecordJobClient.class);
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    private void initialColPosition() throws IOException {
        ConfigureReader cr = new ConfigureReader();
        try {
            colsMap = cr.mappingColumns();
        } catch (IOException ioe) {
            logger.error(
                    "Can't finish the setup process for mapper because the column header map can't be constructed");
        }
        if (colsMap != null && colsMap.size() > 0) {
            for (String col : colsMap.keySet()) {
                int pos = colsMap.get(col);
                config.setInt(col.toUpperCase(), pos);
            }

        } else {
            logger.error("The returned HashMap is empty");
        }
    }
}
