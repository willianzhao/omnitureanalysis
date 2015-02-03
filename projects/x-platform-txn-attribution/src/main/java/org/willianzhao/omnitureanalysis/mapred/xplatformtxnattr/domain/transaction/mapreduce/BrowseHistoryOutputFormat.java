package org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transaction.mapreduce;

import org.willianzhao.omnitureanalysis.mapred.commons.hdfs.StubHubHDFSReader;
import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.mapfile.UserMapFileFactorySmokeTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by willianzhao on 5/13/14.
 */
public class BrowseHistoryOutputFormat extends FileOutputFormat<NullWritable, Text> {

	private static Logger logger = LoggerFactory.getLogger(BrowseHistoryOutputFormat.class);

    @Override
    public RecordWriter<NullWritable, Text> getRecordWriter(
            TaskAttemptContext context) throws IOException, InterruptedException {
        Path file;
        Path dir;
        String browsedetail_output_dir = null;
        Configuration conf = context.getConfiguration();
        browsedetail_output_dir = getBrowseAnalysisPathDir(conf);
        dir = new Path(browsedetail_output_dir);
        file = new Path(dir, FileOutputFormat.getUniqueFile(context,
                "audit_details", ""));
        FileSystem fs = file.getFileSystem(context.getConfiguration());
        if (fs.exists(dir)) {
            fs.delete(dir, true);
            logger.info("Delete the existing HDFS folder {}", browsedetail_output_dir);

        }
        FSDataOutputStream fileOut = fs.create(file, true);
        logger.info("Create HDFS folder {}", browsedetail_output_dir);
        return new BrowseHistoryRecordWriter(fileOut);
    }

    private String getBrowseAnalysisPathDir(Configuration conf) {
        String browsedetail_output_dir = null;
        StubHubHDFSReader pathReader = new StubHubHDFSReader(conf);
        String dataType = conf.get(ProjectConstant.PARAM_LABEL_DATATYPE_RUNTIME);
        String startDateStr = conf.get(ProjectConstant.PARAM_LABEL_STARTDATE);
        String endDateStr = conf.get(ProjectConstant.PARAM_LABEL_ENDDATE);
        if (endDateStr == null || endDateStr.trim().length() == 0) {
            endDateStr = startDateStr;
        }
        try {
            if (dataType.equals(ProjectConstant.TYPE_US_WEB)) {
                browsedetail_output_dir = pathReader.getHDFSOutputPathForBrowseDetail(
                        ProjectConstant.TASK_USCHECKOUT_BROWSEHISTORY, startDateStr, endDateStr);
            } else if (dataType.equals(ProjectConstant.TYPE_STUB_TRANS)) {
                browsedetail_output_dir = pathReader.getHDFSOutputPathForBrowseDetail(
                        ProjectConstant.TASK_USSTUBTRANS_BROWSEHISTORY, startDateStr, endDateStr);
            } else if (dataType.equals(ProjectConstant.TYPE_UK_WEB)) {
                browsedetail_output_dir = pathReader.getHDFSOutputPathForBrowseDetail(
                        ProjectConstant.TASK_UKCHECKOUT_BROWSEHISTORY, startDateStr, endDateStr);
            }

            return browsedetail_output_dir;

        } catch (Exception e) {
            //e.printStackTrace(logger.getStream(Level.ERROR));
            logger.error(
                    "Failed to get hdfs path for browse details. Then return the default path of checkout behavior analysis {}",
                    ProjectConstant.CONF_LABEL_BEHAVIORPATH);
            return conf.get(ProjectConstant.CONF_LABEL_BEHAVIORPATH);
        }
    }
}
