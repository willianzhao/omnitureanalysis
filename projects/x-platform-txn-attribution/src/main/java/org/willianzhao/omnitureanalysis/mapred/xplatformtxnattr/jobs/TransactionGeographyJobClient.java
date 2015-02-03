package org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.jobs;

import java.io.IOException;
import java.util.HashMap;

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
import org.willianzhao.omnitureanalysis.mapred.commons.misc.ConfigureReader;
import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;
import org.willianzhao.omnitureanalysis.mapred.commons.model.TransactionGeography;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transByTimezone.geoParse.mapreduce.GeoParseMapper;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transByTimezone.geoParse.mapreduce.GeoParseReducer;

/**
 * Created by weilzhao on 8/21/14.
 */
public class TransactionGeographyJobClient extends Configured implements Tool {
    Configuration config;
    HashMap<String, Integer> colsMap = null;
	private static Logger logger = LoggerFactory.getLogger(TransactionGeographyJobClient.class);

    @Override
    public int run(String[] strings) throws Exception {
        config = getConf();
        initialColPosition();
        StubHubHDFSReader pathReader = new StubHubHDFSReader(config);
        String jobName = config.get(ProjectConstant.PARAM_LABEL_JOBNAME_RUNTIME);
        String transactionDateStr = config.get(ProjectConstant.PARAM_LABEL_TRANSDATE_RUNTIME);
        //Get input and output Path
        String transGeo_inputpath = null;
        String transGeo_outputpath = null;
        transGeo_inputpath = pathReader.getHDFSOutputPathForTransjobs(ProjectConstant.PATH_ALL_TRANS_RECORD, transactionDateStr);
        transGeo_outputpath = pathReader.getHDFSOutputPathForTransjobs(ProjectConstant.PATH_ALL_TRANS_GEOGRAPHY, transactionDateStr);
        pathReader.removePathIfExisted(transGeo_outputpath);
        Job job = Job.getInstance(config, jobName);
        job.setMapperClass(GeoParseMapper.class);
        job.setReducerClass(GeoParseReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TransactionGeography.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(TransactionGeography.class);
        FileInputFormat.addInputPaths(job, transGeo_inputpath);
        job.setInputFormatClass(TextInputFormat.class);
        Path dir = new Path(transGeo_outputpath);
        FileOutputFormat.setOutputPath(job, dir);
        job.setJarByClass(TransactionGeographyJobClient.class);
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
