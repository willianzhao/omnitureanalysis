package org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.jobs;

import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transByTimezone.geoParse.mapreduce.GeoParseMapper;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;

/**
 * Created by willianzhao on 3/22/14.
 */
public class ParameterParser {

    private Configuration conf;
    private String[] inputParams;
	private static Logger logger = LoggerFactory.getLogger(ParameterParser.class);

    public ParameterParser(Configuration conf, String[] inputParams) {
        this.conf = conf;
        this.inputParams = inputParams;
    }

    public void parseParam() throws Exception {
        String date_pattern = "yyyyMMdd";
        SimpleDateFormat simpleFormatter = new SimpleDateFormat(date_pattern);
        String startDateStr = null;
        String endDateStr = null;
        int durationDays = -1;
        String durationDaysStr = null;
        String target = null;
        String mode = null;
        for (int i = 0; i < inputParams.length; ++i) {
            if ("-s".equals(inputParams[i])) {
                try {
                    i++;

                    /*validate the input parameter date format*/
                    simpleFormatter.parse(inputParams[i]);
                    startDateStr = inputParams[i];
                    conf.set(ProjectConstant.PARAM_LABEL_STARTDATE, startDateStr);

                } catch (ParseException pe) {
                    logger.error("Fail to parse -s parameter {} as date format {}", inputParams[i], date_pattern);
                    //pe.printStackTrace(logger.getStream(Level.ERROR));
                    throw new Exception("Can't parse start date parameter");
                }

            } else if ("-n".equals(inputParams[i])) {
                i++;
                durationDays = Integer.parseInt(inputParams[i]);
                durationDaysStr = inputParams[i];
                if (durationDays >= 0) {
                    conf.set(ProjectConstant.PARAM_LABEL_DURATION, durationDaysStr);
                } else {
                    logger.error(
                            "Input parameter duration should be equal with or larger than zero. Now accept the value as {}",
                            durationDays);
                    throw new Exception("Input parameter duration is not equal with or larger than zero");

                }

            } else if ("-t".equals(inputParams[i])) {
                i++;
                target = inputParams[i].toUpperCase();
                if (Arrays.asList(ProjectConstant.DATATYPELIST).contains(target) || target.equals(
                        ProjectConstant.TYPE_ALL)) {
                    conf.set(ProjectConstant.PARAM_LABEL_DATATYPE, target);

                } else {
                    logger.error(
                            "Input parameter data type should be as one of the values {} or 'ALL'. Now accept the value as {}",
                            Arrays.toString(ProjectConstant.DATATYPELIST), target);
                    throw new Exception("Input parameter data type is not in the validate value list");

                }
            } else if ("-m".equals(inputParams[i])) {
                i++;
                mode = inputParams[i].toLowerCase();
                if (Arrays.asList(ProjectConstant.MODELIST).contains(mode)) {
                    conf.set(ProjectConstant.PARAM_LABEL_MODE, mode);

                } else {
                    logger.error(
                            "Input parameter 'mode' should be as one of the values '{}'. Now accept the value as {}",
                            Arrays.toString(ProjectConstant.MODELIST), mode);
                    throw new Exception("Input parameter 'mode' is not in the validate value list");

                }
            } else if ("-e".equals(inputParams[i])) {
                try {
                    i++;

                    /*validate the input parameter date format*/
                    simpleFormatter.parse(inputParams[i]);
                    endDateStr = inputParams[i];
                    conf.set(ProjectConstant.PARAM_LABEL_ENDDATE, endDateStr);

                } catch (ParseException pe) {
                    logger.error("Fail to parse -s parameter {} as date format {}", inputParams[i], date_pattern);
                    //pe.printStackTrace(logger.getStream(Level.ERROR));
                    throw new Exception("Can't parse end date parameter");
                }

            } else if ("-debug".equals(inputParams[i])) {
                conf.set(ProjectConstant.PARAM_LABEL_DEBUG, ProjectConstant.CONSTANT_TRUE);
                i++;
            } else {
                logger.error("Input parameter can't be recognized. {}", ProjectConstant.getUsage());
                throw new Exception("Can't recognize the input parameter");

            }
        }

    }
}
