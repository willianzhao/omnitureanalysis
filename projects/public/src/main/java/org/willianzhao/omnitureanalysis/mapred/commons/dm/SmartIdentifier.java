package org.willianzhao.omnitureanalysis.mapred.commons.dm;

import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;
import org.apache.hadoop.conf.Configuration;

/**
 * Created by willianzhao on 3/25/14.
 */
public class SmartIdentifier {
    Configuration conf;
    String[] rawFields;
    String dataType;

    SmartIdentifier(Configuration config, String[] inputFields) {
        conf = config;
        rawFields = inputFields;
        dataType = conf.get(ProjectConstant.PARAM_LABEL_DATATYPE);
    }
}
