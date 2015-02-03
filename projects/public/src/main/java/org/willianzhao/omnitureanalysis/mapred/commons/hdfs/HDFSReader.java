package org.willianzhao.omnitureanalysis.mapred.commons.hdfs;

import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by willianzhao on 3/20/14.
 */
public abstract class HDFSReader {

    public String getHDFSInputPath(String mode, String dataType, String startDate, String endDate) throws Exception {
        if (mode.equalsIgnoreCase(ProjectConstant.MODE_VISIT)) {
            return this.getHDFSPathForVisit(ProjectConstant.INPUT_DIRECTION, dataType, startDate);
        } else if (mode.equalsIgnoreCase(ProjectConstant.MODE_VISITOR)) {
            return this.getHDFSPathForVisitor(ProjectConstant.INPUT_DIRECTION, startDate);
        } else if (mode.equalsIgnoreCase(ProjectConstant.MODE_VISITPATH)) {
            return this.getHDFSPathForVisitpath(dataType, startDate);
        } else {
            return null;
        }
    }

    public String getHDFSOutputPath(String mode, String dataType, String startDate, String endDate) throws Exception {
        if (mode.equalsIgnoreCase(ProjectConstant.MODE_VISIT)) {
            return this.getHDFSPathForVisit(ProjectConstant.OUTPUT_DIRECTION, dataType, startDate);
        } else if (mode.equalsIgnoreCase(ProjectConstant.MODE_VISITOR)) {
            return this.getHDFSPathForVisitor(ProjectConstant.OUTPUT_DIRECTION, startDate);
        } else if (mode.equalsIgnoreCase(ProjectConstant.MODE_VISITPATH)) {
            return this.getHDFSPathForVisitpath(dataType, startDate);
        } else if (mode.equalsIgnoreCase(ProjectConstant.MODE_BUYBEHAVIOR_OMNI)) {
            if (dataType.equals(ProjectConstant.TYPE_US_WEB)) {
                return this.getHDFSOutputPathForBehavior(ProjectConstant.TASK_USCHECKOUT_BROWSEHISTORY, startDate, endDate);
            } else {
                return this.getHDFSOutputPathForBehavior(ProjectConstant.TASK_UKCHECKOUT_BROWSEHISTORY, startDate, endDate);
            }
        } else if (mode.equalsIgnoreCase(ProjectConstant.MODE_BUYBEHAVIOR_STUBTRANS)) {
            if (dataType.equals(ProjectConstant.TYPE_STUB_TRANS)) {
                return this.getHDFSOutputPathForBehavior(ProjectConstant.TASK_USSTUBTRANS_BROWSEHISTORY, startDate, endDate);
            } else {
                return this.getHDFSOutputPathForBehavior(ProjectConstant.TASK_UKSTUBTRANS_BROWSEHISTORY, startDate, endDate);
            }
        } else {
            return null;
        }

    }

    abstract public String getGenreNodesFileLoc() throws Exception;

    abstract public String getEventFileLoc() throws Exception;

    abstract String getHDFSPathForVisitpath(String dateType, String dateStr) throws Exception;

    abstract String getHDFSPathForVisit(String direction, String dateType, String dateStr) throws Exception;

    abstract String getHDFSPathForVisitor(String direction, String dateStr) throws Exception;

    abstract String getHDFSOutputPathForBehavior(String task, String startDate, String endDate) throws Exception;

    abstract public Properties getConfPropInstance() throws IOException;

    abstract public String getHDFSOutputPathForBrowseDetail(String task, String startDate, String endDate) throws Exception;
}
