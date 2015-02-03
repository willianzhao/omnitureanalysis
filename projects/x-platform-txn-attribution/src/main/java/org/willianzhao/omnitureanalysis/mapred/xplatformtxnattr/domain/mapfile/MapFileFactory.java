package org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.mapfile;

import org.willianzhao.omnitureanalysis.mapred.commons.dm.GenreIdentifier;
import org.willianzhao.omnitureanalysis.mapred.commons.hdfs.StubHubHDFSReader;
import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by willianzhao on 5/15/14.
 */
public abstract class MapFileFactory {

    Configuration config = null;
    StubHubHDFSReader hdfsReader = null;
    FileSystem fs = null;
    Calendar lastProcessedCal = null;
    Calendar maxProcessedCal = null;
    Calendar paramEndDateCal = null;
    MapFile.Writer writer = null;
    MapFile.Reader lookupReader = null;

	private static Logger logger = LoggerFactory.getLogger(MapFileFactory.class);
    SimpleDateFormat paramDateFormatter = new SimpleDateFormat(
            ProjectConstant.CONSTANT_PARAM_DATE_PATTERN);
    SimpleDateFormat timestampFormatter = new SimpleDateFormat(ProjectConstant.CONSTANT_DATE_PATTERN);
    String outputPath = null;

    public abstract void loadMapFile() throws Exception;

    public abstract MapFile.Reader getMapfileReader() throws Exception;

    public MapFileFactory() {
    }

    protected MapFileFactory(Configuration conf) {
        this.config = conf;
        String endDateStr = config.get(ProjectConstant.PARAM_LABEL_ENDDATE);
        paramEndDateCal = Calendar.getInstance();
        SimpleDateFormat paramDateFormatter = new SimpleDateFormat(
                ProjectConstant.CONSTANT_PARAM_DATE_PATTERN);
        Date requiredTimeWindowBound = null;
        try {
            requiredTimeWindowBound = paramDateFormatter.parse(endDateStr);
        } catch (ParseException e) {
            //do nothing
        }
        paramEndDateCal.setTime(requiredTimeWindowBound);
    }


    protected void loadLastProcessedTime() throws Exception {
        String lastChangeTimestamp;
        if (fs == null) {
            this.fs = FileSystem.get(config);
        }
        Path path = new Path(outputPath + "/" + ProjectConstant.PATH_MAPFILE_LASTCHNAGE);
        if (fs.exists(path)) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            lastChangeTimestamp = br.readLine();
        } else {
            lastChangeTimestamp = ProjectConstant.CONSTANT_DATE_MIN;
            logger.debug("Can't find the path {}. It's the first time to create the mapfile. Give a default value '{}' as the last time stamp", path, lastChangeTimestamp);
        }
        Date processEndDate = timestampFormatter.parse(lastChangeTimestamp);
        lastProcessedCal = Calendar.getInstance();
        lastProcessedCal.setTime(processEndDate);
    }

    protected boolean checkIfMapFileExisted() {
        logger.debug("Check the HDFS folder on {}", outputPath);
        Path path = new Path(outputPath);
        boolean mapfileExisted = false;
        try {
            if (fs == null) {
                this.fs = FileSystem.get(config);
            }
            FileStatus status = fs.getFileStatus(path);
            if (status.isDir()) {
                mapfileExisted = true;
            }
        } catch (IOException e) {
            logger.error("Failed to check mapfile under {}", outputPath);
        }
        return mapfileExisted;
    }

    protected boolean checkIfNeedMapFileToCreate() {

        Path path = new Path(outputPath);
        boolean toCreateMapFile = false;
        try {
            if (fs == null) {
                this.fs = FileSystem.get(config);
            }
            if (fs.exists(path)) {
                // logger.trace("Mapfile {} is already existed.", outputPath);
                if (fs.exists(path)) {
                    try {
                        if (lastProcessedCal == null) {
                            //Set last processed timestamp calendar
                            loadLastProcessedTime();
                        }
                        if (paramEndDateCal.after(lastProcessedCal)) {
                            toCreateMapFile = true;
                            logger.info("The existing mapfile can't cover the required time-window");
                        } else {
                            //The existing mapfile can cover the required time-window
                            toCreateMapFile = false;
                            logger.debug("Last processed time stamp : {}, Accept parameter date : {}", lastProcessedCal.getTime(), paramEndDateCal.getTime());
                            logger.info("The existing mapfile can cover the required time-window");
                        }
                    } catch (Exception e) {
                        //Report the error and terminate processing
                        //e.printStackTrace(logger.getStream(Level.ERROR));

                    }

                } else {
                    toCreateMapFile = true;
                    logger.info("The timestamp file is not existed for the mapfile hence recreate the mapfile");
                }
            } else {
                toCreateMapFile = true;
            }

        } catch (IOException ioe) {
            logger.error("Error when checking the status of mapfile HDFS path {}", outputPath);
            toCreateMapFile = false;
        }
        return toCreateMapFile;
    }

    protected void markTimestamp(String mapFileOutputPath) throws IOException {
        Path path = new Path(mapFileOutputPath + "/" + ProjectConstant.PATH_MAPFILE_LASTCHNAGE);
        BufferedWriter outbw = null;
        Date todaysDate = null;
        try {
            if (fs != null) {
                FSDataOutputStream fout = fs.create(path);
                outbw = new BufferedWriter(new OutputStreamWriter(fout));
                if (paramEndDateCal == null || lastProcessedCal == null) {
                    logger.debug("if the mapfile is initial created, then use the last processed path date as time stamp");
                    todaysDate = maxProcessedCal.getTime();
                } else if (paramEndDateCal.before(lastProcessedCal)) {
                    logger.debug("The existing time stamp '{}'is later than end date '{}' then skip marking time stamp", lastProcessedCal.getTime(), paramEndDateCal.getTime());
                    return;
                } else {
                    logger.debug("The end date '{}' is later than existing time stamp '{}'. Replace it with the last processed date {}", paramEndDateCal.getTime(), lastProcessedCal.getTime(), maxProcessedCal.getTime());
                    todaysDate = maxProcessedCal.getTime();
                }
                SimpleDateFormat timestampFormatter = new SimpleDateFormat(ProjectConstant.CONSTANT_DATE_PATTERN);
                String lastChangeTimestamp = timestampFormatter.format(todaysDate);
                outbw.write(lastChangeTimestamp);
                outbw.newLine();
                outbw.flush();
                logger.info("Write down the last modified timestamp");
            }
        } catch (IOException e) {
            //e.printStackTrace(logger.getStream(Level.ERROR));
        } finally {
            if (outbw != null) {
                outbw.close();
            }
        }
    }

    protected boolean isForIncrementalLoading(Path filePath) throws Exception {
        String pathTimeStr = null;

        if (lastProcessedCal == null) {
            //Set last processed timestamp calendar
            loadLastProcessedTime();
        }
        pathTimeStr = getDateStringFromPath(filePath);
        Date dateInPath = paramDateFormatter.parse(pathTimeStr);
        Calendar dateInPathCal = Calendar.getInstance();
        dateInPathCal.setTime(dateInPath);
        boolean inScope = false;
        if (dateInPathCal.after(lastProcessedCal)) {
            inScope = true;
//            if (paramEndDateCal != null && dateInPathCal.after(paramEndDateCal)) {
//                inScope = false;
//            }
        }

        return inScope;
    }

    String getDateStringFromPath(Path filePath) {
        String pathStr;
        int lastSlashPos;
        int startIndex;
        String pathTimeStr;
        pathStr = filePath.toString();
//        pathLength = pathStr.length();
        lastSlashPos = pathStr.lastIndexOf(File.separator);
        startIndex = lastSlashPos - ProjectConstant.CONSTANT_PARAM_DATE_PATTERN_LENTH;
//        logger.debug("Start to process path {} by extracting substring from position {} to {}", pathStr,startIndex,lastSlashPos);
        pathTimeStr = pathStr.substring(startIndex, lastSlashPos);
        return pathTimeStr;
    }

    void recordMaxProcessedCal(Path path) throws ParseException {
        String pathTimeStr = getDateStringFromPath(path);
        Date dateInPath = paramDateFormatter.parse(pathTimeStr);
        if (maxProcessedCal == null) {
            maxProcessedCal = Calendar.getInstance();
            maxProcessedCal.setTime(dateInPath);
        } else {
            Calendar dateInPathCal = Calendar.getInstance();
            dateInPathCal.setTime(dateInPath);
            if (dateInPathCal.after(maxProcessedCal)) {
                maxProcessedCal.setTime(dateInPath);
            }
        }
    }
}
