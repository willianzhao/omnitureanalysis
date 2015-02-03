package org.willianzhao.omnitureanalysis.mapred.commons.model;

import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Created by weilzhao on 8/25/14.
 */
public class MRBaseFactory {

    Configuration conf;
    String dataSource;

    public MRBaseFactory(Configuration conf, Path inputSplitPath) throws Exception {
        this.conf = conf;
        String inputPathStr = inputSplitPath.toString().toUpperCase();
        identifyDataSource(inputPathStr);

    }

    public String getDataSource() {
        return dataSource == null ? "NODEFINED" : dataSource;
    }

    private void identifyDataSource(String inputPathStr) {
        if (inputPathStr.indexOf(ProjectConstant.TYPE_STUB_TRANS) > -1) {
            dataSource = ProjectConstant.SOURCE_STUB_TRANS;
        } else if (inputPathStr.indexOf(ProjectConstant.TYPE_US_WEB) > -1) {
            dataSource = ProjectConstant.SOURCE_US_WEB;
        } else if (inputPathStr.indexOf(ProjectConstant.TYPE_US_MWEB) > -1) {
            dataSource = ProjectConstant.SOURCE_US_MWEB;

        } else if (inputPathStr.indexOf(ProjectConstant.TYPE_IPAD_APPS) > -1) {
            dataSource = ProjectConstant.SOURCE_IPAD;
        } else if (inputPathStr.indexOf(ProjectConstant.TYPE_IOS_APPS) > -1) { 
            dataSource = ProjectConstant.SOURCE_IOS; 	//set the source as IPAD even for IOS
        }else if (inputPathStr.indexOf(ProjectConstant.TYPE_MOBILE_APPS) > -1) {
            dataSource = ProjectConstant.SOURCE_MOBILE;
        } else if (inputPathStr.indexOf(ProjectConstant.TYPE_UK_MWEB) > -1) {
            dataSource = ProjectConstant.SOURCE_UK_MWEB;

        } else if (inputPathStr.indexOf(ProjectConstant.TYPE_UK_WEB) > -1) {
            dataSource = ProjectConstant.SOURCE_UK_WEB;
        } else {
            dataSource = ProjectConstant.SOURCE_OTHERS;
        }
    }

}
