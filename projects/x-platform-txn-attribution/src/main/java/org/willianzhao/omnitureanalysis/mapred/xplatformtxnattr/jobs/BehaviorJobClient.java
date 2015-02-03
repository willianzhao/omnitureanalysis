package org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.jobs;

import org.willianzhao.omnitureanalysis.mapred.commons.hdfs.StubHubHDFSReader;
import org.willianzhao.omnitureanalysis.mapred.commons.misc.ConfigureReader;
import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;
import org.willianzhao.omnitureanalysis.mapred.commons.model.VisitStep;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.mapfile.EventsMapFileFactory;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transByTimezone.geoParse.mapreduce.GeoParseMapper;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transaction.mapreduce.BrowseHistoryMapper;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transaction.mapreduce.BrowseHistoryOutputFormat;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transaction.mapreduce.BrowseHistoryReducer;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by willianzhao on 5/21/14.
 */
public class BehaviorJobClient extends Configured implements Tool {

    Configuration config;
    HashMap<String, Integer> colsMap = null;
	private static Logger logger = LoggerFactory.getLogger(GeoParseMapper.class);

    @Override
    public int run(String[] strings) throws Exception {
        config = getConf();
        initialColPosition();
        String duration = config.get(ProjectConstant.PARAM_LABEL_DURATION);
        if (duration == null) {
            duration = ProjectConstant.LOOKAHEAD_DAYS;
        }
        //Add one more day in mobile data scope is to convert PDT time to UTC
        duration=String.valueOf(Integer.parseInt(duration)+1);
        config.set(ProjectConstant.PARAM_LABEL_DURATION,duration);
        logger.info("We look back for {} days for mobile browsing history (considering the UTC time conversion)", duration);
        String mDataType = "";
        if (config.get(ProjectConstant.PARAM_LABEL_DATATYPE_RUNTIME).equals(ProjectConstant.TYPE_US_WEB)) {
            mDataType = ProjectConstant.TYPE_US_WEB;
        } else if (config.get(ProjectConstant.PARAM_LABEL_DATATYPE_RUNTIME).equals(ProjectConstant.TYPE_STUB_TRANS)) {
            mDataType = ProjectConstant.TYPE_STUB_TRANS;
        }
        StubHubHDFSReader pathReader = new StubHubHDFSReader(config);
        String startDateStr = config.get(ProjectConstant.PARAM_LABEL_STARTDATE);
        String endDateStr = config.get(ProjectConstant.PARAM_LABEL_ENDDATE);
        if (endDateStr == null || endDateStr.trim().length() == 0) {
            endDateStr = startDateStr;
        }
        String jobName = "SH_Usweb_TransAnalysis_" + startDateStr + "_" + endDateStr;
        String data_inputpath = null;
        String data_outputpath = null;
        try {
            data_inputpath = pathReader.getHDFSInputPathForBehavior(mDataType, startDateStr, endDateStr, duration);
            if (mDataType.equals(ProjectConstant.TYPE_US_WEB)) {
                data_outputpath = pathReader.getHDFSOutputPath(ProjectConstant.MODE_BUYBEHAVIOR_OMNI,
                        ProjectConstant.TYPE_US_WEB, startDateStr, endDateStr);
            } else if (mDataType.equals(ProjectConstant.TYPE_STUB_TRANS)) {
                data_outputpath = pathReader.getHDFSOutputPath(ProjectConstant.MODE_BUYBEHAVIOR_STUBTRANS,
                        ProjectConstant.TYPE_STUB_TRANS, startDateStr, endDateStr);
            }
            config.set(ProjectConstant.CONF_LABEL_BEHAVIORPATH, data_outputpath);

        } catch (Exception e) {
            logger.error("Failed to get hdfs path for job {}", jobName);
            //e.printStackTrace(logger.getStream(Level.ERROR));
            throw new IOException("Can't get hdfs path");
        }
        Job job = Job.getInstance(config, jobName);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(VisitStep.class);
        job.setMapperClass(BrowseHistoryMapper.class);
        job.setReducerClass(BrowseHistoryReducer.class);
        FileInputFormat.addInputPaths(job, data_inputpath);
        job.setInputFormatClass(TextInputFormat.class);
        Path dir = new Path(data_outputpath);
        FileSystem fs = dir.getFileSystem(config);
        if (fs.exists(dir)) {
            fs.delete(dir, true);
            logger.trace("Delete the existing HDFS folder {}", data_outputpath);

        }
        FileOutputFormat.setOutputPath(job, dir);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        String debugFlag = config.get(ProjectConstant.PARAM_LABEL_DEBUG);
        if (debugFlag != null && debugFlag.equals(ProjectConstant.CONSTANT_TRUE)) {
            logger.info("Turn on the details output");
            MultipleOutputs.addNamedOutput(job, ProjectConstant.TASK_CHECKOUT_BROWSEDETAIL,
                    BrowseHistoryOutputFormat.class, NullWritable.class, Text.class);
        }
        EventsMapFileFactory eventMapFile = new EventsMapFileFactory(config);
        eventMapFile.loadMapFile();
        job.setJarByClass(BehaviorJobClient.class);
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
