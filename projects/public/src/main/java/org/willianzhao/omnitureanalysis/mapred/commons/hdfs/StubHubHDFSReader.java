package org.willianzhao.omnitureanalysis.mapred.commons.hdfs;

import org.willianzhao.omnitureanalysis.mapred.commons.dm.GenreIdentifier;
import org.willianzhao.omnitureanalysis.mapred.commons.misc.ConfigureReader;
import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

/**
 * Created by weilzhao on 3/17/2014.
 */
public class StubHubHDFSReader extends HDFSReader {

    Properties property;
    Configuration conf;
	private static Logger logger = LoggerFactory.getLogger(StubHubHDFSReader.class);

    
    public StubHubHDFSReader(Configuration conf) {
        this.conf = conf;
    }

    public String getHDFSInputPathForTransRecord(String transactionDateStr) throws ParseException, IOException {
        String tagName = null;
        String sourceFolderStr = null;
        String rootFolderStr = null;
        String pathStr = null;
        StringBuilder sb = new StringBuilder();
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat simpleFormatter = new SimpleDateFormat(ProjectConstant.CONSTANT_PARAM_DATE_PATTERN);
        Date processStartDate = simpleFormatter.parse(transactionDateStr);
        //Consider timezone differ, extend the end date by one more day
        cal.setTime(processStartDate);
        cal.add(Calendar.DATE, 1);
        Date processEndDate = cal.getTime();
        //Get root path
        tagName = "SH_HDFS_INPUT_ROOT";
        rootFolderStr = this.getConfPropInstance().getProperty(tagName);
        //Get input path for Omniture data sources
        for (String dataType : ProjectConstant.DATATYPELIST) {
            tagName = "SH_" + dataType;
            sourceFolderStr = this.getConfPropInstance().getProperty(tagName);
            processStartDate = simpleFormatter.parse(transactionDateStr);
            while (!processStartDate.after(processEndDate)) {
                String currentDay = simpleFormatter.format(processStartDate);
                pathStr = rootFolderStr + "/" + sourceFolderStr + "/" + currentDay;
                if (!isPathExisted(pathStr)) {
                    logger.error("The HDFS input path for {} : {} is not existed.", tagName, pathStr);
//                    throw new IOException("Input path " + pathStr + " is missing");
                } else {
                    if (sb.toString().length() == 0) {
                        sb.append(pathStr);
                    } else {
                        sb.append(",").append(pathStr);
                    }

                }
                cal.setTime(processStartDate);
                cal.add(Calendar.DATE, 1);
                processStartDate = cal.getTime();
            }
        }
        //Get input path for stub_trans data source
        processStartDate = simpleFormatter.parse(transactionDateStr);
        String currentDay = simpleFormatter.format(processStartDate);
        pathStr = this.getHDFSOutputPathForStubTrans(currentDay,ProjectConstant.MODE_STUBTRANS_FEED_ALL);
        if (!isPathExisted(pathStr)) {
            logger.error("The HDFS input path for stub_trans : {} is not existed.", pathStr);
            throw new IOException("Input path " + pathStr + " is missing");
        } else {
            if (sb.toString().length() == 0) {
                sb.append(pathStr);
            } else {
                sb.append(",").append(pathStr);
            }
        }
        final String finalInputPath = sb.toString();
        logger.debug("The final input path is {}", finalInputPath);
        return finalInputPath;
    }

    /*
    @Param sourceFolderStr : the value can be ProjectConstant.{PATH_TRANS_RECORD, PATH_TRANS_LOCALIZATION and PATH_TRANS_GEOGRAPHY}.
     */
    public String getHDFSOutputPathForTransjobs(String sourceFolderStr, String transactionDateStr) throws IOException {
        String tagName = null;
        String rootFolderStr = null;
        tagName = "SH_HDFS_OUTPUT_ROOT";
        rootFolderStr = this.getConfPropInstance().getProperty(tagName);
        String pathStr = rootFolderStr + "/" + sourceFolderStr + "/" + transactionDateStr;
        logger.debug("Get output folder for {} data as '{}' on date {}", sourceFolderStr, rootFolderStr, transactionDateStr);
        return pathStr;
    }

    public String getHDFSInputPathForBehavior(String dataType, String startDate, String endDate, String duration) throws Exception {
        String tagName = null;
        String sourceFolderStr = null;
        String rootFolderStr = null;
        String pathStr = null;
        StringBuilder sb = new StringBuilder();
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat simpleFormatter = new SimpleDateFormat(ProjectConstant.CONSTANT_PARAM_DATE_PATTERN);
        Date processStartDate = simpleFormatter.parse(startDate);
        Date processEndDate = simpleFormatter.parse(endDate);
        tagName = "SH_HDFS_INPUT_ROOT";
        rootFolderStr = this.getConfPropInstance().getProperty(tagName);
        tagName = "SH_" + dataType;
        sourceFolderStr = this.getConfPropInstance().getProperty(tagName);
        try {
            while (!processStartDate.after(processEndDate)) {
                String currentDay = simpleFormatter.format(processStartDate);
                if (dataType.equals(ProjectConstant.TYPE_STUB_TRANS)) {
                    pathStr = this.getHDFSOutputPathForStubTrans(currentDay,ProjectConstant.MODE_STUBTRANS_FEED_USWEB);

                } else {
                    pathStr = rootFolderStr + "/" + sourceFolderStr + "/" + currentDay;
                }
                if (!isPathExisted(pathStr)) {
                    logger.error("The HDFS input path for {} : {} is not existed.", tagName, pathStr);
                    throw new IOException("Input path " + pathStr + " is missing");

                } else {
                    if (sb.toString().length() == 0) {
                        sb.append(pathStr);
                    } else {
                        sb.append(",").append(pathStr);
                    }

                }
                cal.setTime(processStartDate);
                cal.add(Calendar.DATE, 1);
                processStartDate = cal.getTime();
            }
            logger.info("Construct the {} PC desktop raw data path as {}", dataType, sb.toString());

        } catch (IOException e) {
            logger.error("Fail to get property {} from configure file", tagName);
            //e.printStackTrace(logger.getStream(Level.ERROR));
            throw new Exception("Can't find the data source folder");
        }
        ArrayList<String> mobileDataScopeList = new ArrayList<String>();
        if (dataType.equals(ProjectConstant.TYPE_US_WEB) || dataType.equals(ProjectConstant.TYPE_STUB_TRANS)) {
            mobileDataScopeList.add(ProjectConstant.TYPE_US_MWEB);
            mobileDataScopeList.add(ProjectConstant.TYPE_IPAD_APPS);
            mobileDataScopeList.add(ProjectConstant.TYPE_MOBILE_APPS);
            mobileDataScopeList.add(ProjectConstant.TYPE_IOS_APPS);
        } else if (dataType.equals(ProjectConstant.TYPE_UK_WEB)) {
            mobileDataScopeList.add(ProjectConstant.TYPE_UK_MWEB);
        }
        for (String mobileDataType : mobileDataScopeList) {
            try {
                tagName = "SH_" + mobileDataType;
                sourceFolderStr = this.getConfPropInstance().getProperty(tagName);
                processStartDate = simpleFormatter.parse(startDate);
                processEndDate = simpleFormatter.parse(endDate);
                cal.setTime(processStartDate);
                //Because duration has been added one more day so we will look ahead one day more considering PDT to UTC conversion
                cal.add(Calendar.DATE, -Integer.parseInt(duration));
                processStartDate = cal.getTime();
                while (!processStartDate.after(processEndDate)) {
                    String currentDay = simpleFormatter.format(processStartDate);
                    pathStr = rootFolderStr + "/" + sourceFolderStr + "/" + currentDay;
                    if (!isPathExisted(pathStr)) {
                        logger.info("The HDFS input path for {} : {} is not existed. Skip it.", tagName, pathStr);
                    } else {
                        if (sb.toString().length() == 0) {
                            sb.append(pathStr);
                        } else {
                            sb.append(",").append(pathStr);
                        }
                        logger.info("Add the mobile {} raw data path as {}", sourceFolderStr, pathStr);

                    }
                    cal.setTime(processStartDate);
                    cal.add(Calendar.DATE, 1);
                    processStartDate = cal.getTime();

                }

            } catch (IOException e) {
                logger.error("Fail to get property {} from configure file", tagName);
                //e.printStackTrace(logger.getStream(Level.ERROR));
                throw new Exception("Can't find the data source folder");
            } catch (ParseException pe) {
                //pe.printStackTrace(logger.getStream(Level.ERROR));
                throw new Exception("Can't parse start date parameter");
            }
        }
        String inputPath = sb.toString();
        logger.info("The input path is '{}'", inputPath);
        return inputPath;
    }

    public String getMapFileInputPathRoot(String task) throws Exception {
        String rootFolderStr = null;
        try {
            if (task.equals(ProjectConstant.TASK_MAPFILE_EVENTS)) {
                rootFolderStr = this.getConfPropInstance().getProperty(ProjectConstant.PATH_EVENTS_MAPFILE);

            } else if (task.equals(ProjectConstant.TASK_MAPFILE_TICKETS)) {
                rootFolderStr = this.getConfPropInstance().getProperty(ProjectConstant.PATH_TICKETS_MAPFILE);

            } else if (task.equals(ProjectConstant.TASK_MAPFILE_USERS)) {
                rootFolderStr = this.getConfPropInstance().getProperty(ProjectConstant.PATH_USERS_MAPFILE);

            } else if (task.equals(ProjectConstant.TASK_MAPFILE_USER_CONTACTS)) {
                rootFolderStr = this.getConfPropInstance().getProperty(ProjectConstant.PATH_USER_CONTACTS_FILE);

            } else if (task.equals(ProjectConstant.TASK_MAPFILE_GEONAMES_ZIPCODE)) {
                rootFolderStr = this.getConfPropInstance().getProperty(ProjectConstant.PATH_RESOURCE_ROOT);

            } else {
                throw new Exception("The input parameter for task '" + task + "' is unrecognized");
            }

        } catch (IOException e) {
            logger.error("Fail to get property {} from configure file", task);
            //e.printStackTrace(logger.getStream(Level.ERROR));
            throw new Exception("Can't find the data folder");
        }
        return rootFolderStr;
    }

    public String getHDFSInputPathForStubTrans(String transactionDateStr) throws IOException, ParseException {
        String rootFolderStr = this.getConfPropInstance().getProperty(ProjectConstant.PATH_STUBTRANS_FILE);
        String inputPath = "";

        /*
        Because of the difference of timezone, we need to include one more day (prior day) for stub_trans data
         */
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat simpleFormatter = new SimpleDateFormat(ProjectConstant.CONSTANT_PARAM_DATE_PATTERN);
        Date transDate = simpleFormatter.parse(transactionDateStr);
        int i = ProjectConstant.STUBTRANS_LOOKAHEAD_DAYS;
        cal.setTime(transDate);
        cal.add(Calendar.DATE, i);
        transDate = cal.getTime();
        while (true) {
            cal.setTime(transDate);
            cal.add(Calendar.DATE, 1);
            transDate = cal.getTime();
            String newPath = rootFolderStr + "/" + simpleFormatter.format(transDate);
            if (isPathExisted(newPath)) {
                if (inputPath.length() > 0) {
                    inputPath = inputPath + "," + newPath;
                } else {
                    inputPath = newPath;
                }

            } else {
                break;
            }
            i++;
        }
        logger.info("Get the input path for stub_trans on date {} as '{}'", transactionDateStr, inputPath);
        return inputPath;
    }

    public String getHDFSOutputPathForStubTrans(String transactionDateStr, String mode) throws IOException {
        String tagName;
        String sourceFolderStr="";
        String rootFolderStr;
        if(mode.equals(ProjectConstant.MODE_STUBTRANS_FEED_USWEB)){
            sourceFolderStr= ProjectConstant.PATH_STUBTRANS_USWEB;
        }else if(mode.equals(ProjectConstant.MODE_STUBTRANS_FEED_ALL)){
            sourceFolderStr=ProjectConstant.PATH_STUBTRANS_ALL;
        }
        tagName = "SH_HDFS_OUTPUT_ROOT";
        rootFolderStr = this.getConfPropInstance().getProperty(tagName);
        String pathStr = rootFolderStr + "/" + sourceFolderStr + "/" + transactionDateStr;
        logger.debug("Get output folder for stub_trans feed data as '{}'", rootFolderStr);
        return pathStr;

    }

    public String getMapFileOutputPath(String task) throws Exception {
        return getMapFileOutputPath(task, "");

    }

    public String getMapFileOutputPath(String task, String startDate) throws Exception {
        String tagName = null;
        String sourceFolderStr = ProjectConstant.PATH_MAPFILE;
        String rootFolderStr = null;
        try {
            tagName = "SH_HDFS_OUTPUT_ROOT";
            rootFolderStr = this.getConfPropInstance().getProperty(tagName);

        } catch (IOException e) {
            logger.error("Fail to get property {} from configure file", tagName);
            //e.printStackTrace(logger.getStream(Level.ERROR));
            throw new Exception("Can't find the data folder");
        }
        String pathStr = null;

            /*
            @TODO: path may be changed
             */
        if (task.equals(ProjectConstant.TASK_MAPFILE_EVENTS)) {
            pathStr = rootFolderStr + "/" + sourceFolderStr + "/" + ProjectConstant.TOKEN_MAPFILE_EVENTS + "/" + startDate;
        } else if (task.equals(ProjectConstant.TASK_MAPFILE_TICKETS)) {
            pathStr = rootFolderStr + "/" + sourceFolderStr + "/" + ProjectConstant.TOKEN_MAPFILE_TICKETS + "/" + startDate;
        } else if (task.equals(ProjectConstant.TASK_MAPFILE_USERS)) {
            pathStr = rootFolderStr + "/" + sourceFolderStr + "/" + ProjectConstant.TOKEN_MAPFILE_USERS + "/" + startDate;
        } else if (task.equals(ProjectConstant.TASK_MAPFILE_USER_CONTACTS)) {
            pathStr = rootFolderStr + "/" + sourceFolderStr + "/" + ProjectConstant.TOKEN_MAPFILE_USER_CONTACTS + "/" + startDate;
        } else if (task.equals(ProjectConstant.TASK_MAPFILE_GEONAMES_ZIPCODE)) {
            pathStr = rootFolderStr + "/" + sourceFolderStr + "/" + ProjectConstant.TOKEN_MAPFILE_GEONAMES_ZIPCODE;
        }
        return pathStr;
    }

    public String getHDFSOutputPathForBehavior(String task, String startDate, String endDate) throws Exception {
        String tagName = null;
        String sourceFolderStr = "checkout";
        String rootFolderStr = null;
        try {
            tagName = "SH_HDFS_OUTPUT_ROOT";
            rootFolderStr = this.getConfPropInstance().getProperty(tagName);

        } catch (IOException e) {
            logger.error("Fail to get property {} from configure file", tagName);
            //e.printStackTrace(logger.getStream(Level.ERROR));
            throw new Exception("Can't find the data folder");
        }
        String pathStr = null;

            /*
            @TODO: path may be changed
             */
        if (task.equals(ProjectConstant.TASK_USCHECKOUT_BROWSEHISTORY)) {
            pathStr = rootFolderStr + "/" + sourceFolderStr + "/usweb/" + startDate + "_" + endDate;
        } else if (task.equals(ProjectConstant.TASK_UKCHECKOUT_BROWSEHISTORY)) {
            pathStr = rootFolderStr + "/" + sourceFolderStr + "/ukweb/" + startDate + "_" + endDate;
        } else if (task.equals(ProjectConstant.TASK_USSTUBTRANS_BROWSEHISTORY)) {
            pathStr = rootFolderStr + "/" + sourceFolderStr + "/usweb_stubtrans/" + startDate + "_" + endDate;
        } else if (task.equals(ProjectConstant.TASK_UKSTUBTRANS_BROWSEHISTORY)) {
            pathStr = rootFolderStr + "/" + sourceFolderStr + "/ukweb_stubtrans/" + startDate + "_" + endDate;
        }
        logger.info("Construct the behavior analysis output path as {}", pathStr);
        return pathStr;
    }

    public String getHDFSOutputPathForBrowseDetail(String task, String startDate, String endDate) throws Exception {
        String tagName = null;
        String sourceFolderStr = "audit";
        String rootFolderStr = null;
        try {
            tagName = "SH_HDFS_OUTPUT_ROOT";
            rootFolderStr = this.getConfPropInstance().getProperty(tagName);

        } catch (IOException e) {
            logger.error("Fail to get property {} from configure file", tagName);
            //e.printStackTrace(logger.getStream(Level.ERROR));
            throw new Exception("Can't find the data folder");
        }
        String pathStr = null;

            /*
            @TODO: path may be changed
             */
        if (task.equals(ProjectConstant.TASK_USCHECKOUT_BROWSEHISTORY)) {
            pathStr = rootFolderStr + "/" + sourceFolderStr + "/usweb/" + startDate + "_" + endDate;
        } else if (task.equals(ProjectConstant.TASK_USSTUBTRANS_BROWSEHISTORY)) {
            pathStr = rootFolderStr + "/" + sourceFolderStr + "/usweb_stubtrans/" + startDate + "_" + endDate;
        } else if (task.equals(ProjectConstant.TASK_UKCHECKOUT_BROWSEHISTORY)) {
            pathStr = rootFolderStr + "/" + sourceFolderStr + "/ukweb/" + startDate + "_" + endDate;
        }
        logger.info("Construct the behavior analysis output path as {}", pathStr);
        return pathStr;
    }

    @Override
    public String getGenreNodesFileLoc() throws Exception {
        String fileName = this.getConfPropInstance().getProperty(ProjectConstant.CONF_LABEL_GENREFILE);
        if (fileName != null && fileName.length() > 0) {
            return fileName;
        } else {
            throw new IOException("The genre file setting is not existed in configure file");
        }

    }

    @Override
    public String getEventFileLoc() throws Exception {
        String fileName = this.getConfPropInstance().getProperty(ProjectConstant.CONF_LABEL_EVENTFILE);
        if (fileName != null && fileName.length() > 0) {
            return fileName;
        } else {
            throw new IOException("The event file root path setting is not existed in configure file");
        }
    }

    public String getHDFSPathForVisitpath(String dataType, String dateStr) throws Exception {
        String tagName = null;
        String sourceFolderStr = null;
        String rootFolderStr = null;
        try {
            tagName = "SH_" + dataType;
            sourceFolderStr = this.getConfPropInstance().getProperty(tagName);
            tagName = "SH_HDFS_OUTPUT_ROOT";
            rootFolderStr = this.getConfPropInstance().getProperty(tagName);

        } catch (IOException e) {
            logger.error("Fail to get property {} from configure file", tagName);
            //e.printStackTrace(logger.getStream(Level.ERROR));
            throw new Exception("Can't find the data source folder");
        }
        String pathStr = null;


            /*
            @TODO: path may be changed
             */
        pathStr = rootFolderStr + "/" + ProjectConstant.MODE_VISITPATH + "/" + sourceFolderStr + "/" + dateStr;
        logger.info("Construct the visit path HDFS path as {}" + pathStr);
        return pathStr;
    }

    @Override
    public String getHDFSPathForVisit(String direction, String dataType, String dateStr) throws Exception {
        String tagName = null;
        String sourceFolderStr = null;
        String rootFolderStr = null;
        try {
            tagName = "SH_" + dataType;
            sourceFolderStr = this.getConfPropInstance().getProperty(tagName);
            tagName = "SH_HDFS_" + direction + "_ROOT";
            rootFolderStr = this.getConfPropInstance().getProperty(tagName);

        } catch (IOException e) {
            logger.error("Fail to get property {} from configure file", tagName);
            //e.printStackTrace(logger.getStream(Level.ERROR));
            throw new Exception("Can't find the data source folder");
        }
        String pathStr = null;
        if (direction.equals(ProjectConstant.INPUT_DIRECTION)) {
            pathStr = rootFolderStr + "/" + sourceFolderStr + "/" + dateStr;
            if (!isPathExisted(pathStr)) {
                logger.error("The HDFS input path {} is not existed. Report the error.", pathStr);
                throw new IOException("HDFS input path is not existed.");
            }

        } else if (direction.equals(ProjectConstant.OUTPUT_DIRECTION)) {

            /*
            @TODO : path may be changed
             */
            pathStr = rootFolderStr + "/" + ProjectConstant.MODE_VISIT + "/" + sourceFolderStr + "/" + dateStr;
            // createHDFSPathIfNotExisted(pathStr);
        } else {
            logger.error("Fail to parse the direction parameter {}", direction);
            throw new Exception("Can't recognize the input/output direction");
        }
        logger.info("Construct the visit HDFS path as {}", pathStr);
        return pathStr;
    }

    @Override
    public String getHDFSPathForVisitor(String direction, String dateStr) throws Exception {
        String pathStr = null;
        if (direction.equals(ProjectConstant.INPUT_DIRECTION)) {
            StringBuilder sb = new StringBuilder();

            /*
            The input directories include all types of data source for specified day
             */
            String tempPathStr;
            try {
                tempPathStr = getHDFSPathForVisit(ProjectConstant.OUTPUT_DIRECTION, ProjectConstant.TYPE_US_WEB, dateStr);
                if (!isPathExisted(tempPathStr)) {
                    logger.info("The HDFS input path for us_web : {} is not existed. Skip it.", tempPathStr);
                } else {
                    sb.append(tempPathStr);
                }
                tempPathStr = getHDFSPathForVisit(ProjectConstant.OUTPUT_DIRECTION, ProjectConstant.TYPE_US_MWEB, dateStr);
                if (!isPathExisted(tempPathStr)) {
                    logger.info("The HDFS input path for us_mweb : {} is not existed. Skip it.", tempPathStr);
                } else {
                    if (sb.length() > 0) {
                        sb.append(",").append(tempPathStr);
                    } else {
                        sb.append(tempPathStr);
                    }
                }
                tempPathStr = getHDFSPathForVisit(ProjectConstant.OUTPUT_DIRECTION, ProjectConstant.TYPE_UK_WEB, dateStr);
                if (!isPathExisted(tempPathStr)) {
                    logger.info("The HDFS input path for uk_web : {} is not existed. Skip it.", tempPathStr);
                } else {
                    if (sb.length() > 0) {
                        sb.append(",").append(tempPathStr);
                    } else {
                        sb.append(tempPathStr);
                    }
                }
                tempPathStr = getHDFSPathForVisit(ProjectConstant.OUTPUT_DIRECTION, ProjectConstant.TYPE_UK_MWEB, dateStr);
                if (!isPathExisted(tempPathStr)) {
                    logger.info("The HDFS input path for uk_mweb : {} is not existed. Skip it.", tempPathStr);
                } else {
                    if (sb.length() > 0) {
                        sb.append(",").append(tempPathStr);
                    } else {
                        sb.append(tempPathStr);
                    }
                }
                tempPathStr = getHDFSPathForVisit(ProjectConstant.OUTPUT_DIRECTION, ProjectConstant.TYPE_IPAD_APPS, dateStr);
                if (!isPathExisted(tempPathStr)) {
                    logger.info("The HDFS input path for ipad : {} is not existed. Skip it.", tempPathStr);
                } else {
                    if (sb.length() > 0) {
                        sb.append("," + tempPathStr);
                    } else {
                        sb.append(tempPathStr);
                    }
                }
                tempPathStr = getHDFSPathForVisit(ProjectConstant.OUTPUT_DIRECTION, ProjectConstant.TYPE_IOS_APPS, dateStr);
                if (!isPathExisted(tempPathStr)) {
                    logger.info("The HDFS input path for ios : {} is not existed. Skip it.", tempPathStr);
                } else {
                    if (sb.length() > 0) {
                        sb.append("," + tempPathStr);
                    } else {
                        sb.append(tempPathStr);
                    }
                }
                tempPathStr = getHDFSPathForVisit(ProjectConstant.OUTPUT_DIRECTION, ProjectConstant.TYPE_MOBILE_APPS, dateStr);
                if (!isPathExisted(tempPathStr)) {
                    logger.info("The HDFS input path for mobile : {} is not existed. Skip it.", tempPathStr);
                } else {
                    if (sb.length() > 0) {
                        sb.append(",").append(tempPathStr);
                    } else {
                        sb.append(tempPathStr);
                    }
                }
                pathStr = sb.toString();
                logger.info("Construct the input path for visitor job : {}", pathStr);
            } catch (Exception e) {
                logger.error("Fail to construct the visitor input directory list for date on " + dateStr);
                throw new Exception("Can't construct the input directory list for visitor on " + dateStr);
            }
        } else if (direction.equals(ProjectConstant.OUTPUT_DIRECTION)) {
            String tagName = null;
            String rootFolderStr = null;
            try {
                tagName = "SH_HDFS_" + direction + "_ROOT";
                rootFolderStr = this.getConfPropInstance().getProperty(tagName);

            } catch (IOException e) {
                logger.error("Fail to get property {} from configure file", tagName);
                //e.printStackTrace(logger.getStream(Level.ERROR));
                throw new Exception("Can't find the data source folder");
            }
            pathStr = rootFolderStr + "/" + ProjectConstant.MODE_VISITOR + "/" + dateStr;
            logger.info("Construct the output path for visitor job : {}", pathStr);
        } else {
            logger.error("Fail to parse the direction parameter {}", direction);
            throw new Exception("Can't recognize the input/output direction");
        }
        return pathStr;
    }

    @Override
    public Properties getConfPropInstance() throws IOException {
        if (property == null) {
            ConfigureReader cr = new ConfigureReader();
            property = cr.getLocalConfProperties(ProjectConstant.CONF_SH);
            for (Object key : property.keySet()) {
                String keyItem = (String) key;
                String valueItem = (String) property.get(key);
                conf.set(keyItem, valueItem);
            }

        }
        return property;

    }

    private void createHDFSPathIfNotExisted(String pathStr) throws IOException {
        if (pathStr == null) {
            logger.error("The input path string is NULL");
            throw new IOException("Path String is null");
        }
        Path path = new Path(pathStr);
        FileSystem fs = path.getFileSystem(conf);
        if (!fs.exists(path)) {
            fs.create(path, true);
            logger.info("Created the HDFS path {}", pathStr);
        }

    }

    public boolean removePathIfExisted(String pathStr) {
        Path path = new Path(pathStr);
        try {
            FileSystem fs = path.getFileSystem(conf);
            if (fs.exists(path)) {
                logger.info("Path {} is existed", pathStr);
                fs.delete(path, true);
                logger.info("Delete the existing HDFS folder {}", pathStr);
                return true;
            }

        } catch (IOException ioe) {
            logger.error("Error when checking the status of HDFS path {}", pathStr);
            return false;
        }
        return false;
    }

    public boolean isPathExisted(String pathStr) {
        Path path = new Path(pathStr);
        try {
            FileSystem fs = path.getFileSystem(conf);
            if (fs.exists(path)) {
                return true;
            }

        } catch (IOException ioe) {
            logger.error("Error when checking the status of HDFS path {}", pathStr);
            return false;
        }
        return false;
    }

    public boolean isPathNonEmpty(String pathStr) {
        Path path = new Path(pathStr);
        try {
            FileSystem fs = path.getFileSystem(conf);
            if (fs.exists(path)) {
                FileStatus[] statusList = fs.listStatus(path);
                if (statusList.length > 0) {
                    return true;
                } else {
                    return false;
                }
            }

        } catch (IOException ioe) {
            logger.error("Error when checking the status of HDFS path {}", pathStr);
            return false;
        }
        return false;
    }
}
