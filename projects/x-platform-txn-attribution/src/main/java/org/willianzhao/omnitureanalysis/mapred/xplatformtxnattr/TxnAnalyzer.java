package org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.mapfile.EventsMapFileFactory;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.mapfile.UserContactsMapFileFactory;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.mapfile.UserMapFileFactory;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.mapfile.UserMapFileFactorySmokeTest;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.jobs.BehaviorJobClient;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.jobs.MapFileClient;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.jobs.ParameterParser;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.jobs.StubTransClient;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.jobs.TransRecordJobClient;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.jobs.TransactionGeographyJobClient;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.jobs.TransactionLocalizationJobClient;

/**
 * Created by willianzhao on 3/21/14.
 */
public class TxnAnalyzer {

	private static Logger logger = LoggerFactory.getLogger(TxnAnalyzer.class);

    public static void main(String[] args) throws Exception {
        Date processStartDate = null;
        Date processEndDate = null;
        Configuration configure = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configure, args).getRemainingArgs();
        ParameterParser parameterParser = new ParameterParser(configure, otherArgs);
        parameterParser.parseParam();
        String mode = configure.get(ProjectConstant.PARAM_LABEL_MODE);
        logger.info("Ready to start '{}' job", mode);
        if (mode != null) {
            String startDateStr = configure.get(ProjectConstant.PARAM_LABEL_STARTDATE);
            String endDateStr = configure.get(ProjectConstant.PARAM_LABEL_ENDDATE);
            Calendar cal = Calendar.getInstance();
            SimpleDateFormat simpleFormatter = new SimpleDateFormat(ProjectConstant.CONSTANT_PARAM_DATE_PATTERN);
            if (startDateStr != null && startDateStr.trim().length() == 8) {
                processStartDate = simpleFormatter.parse(startDateStr);
                if (endDateStr == null || endDateStr.trim().length() == 0) {
                    endDateStr = startDateStr;
                }
            }
            if (endDateStr != null && endDateStr.trim().length() == 8) {
                processEndDate = simpleFormatter.parse(endDateStr);
            }
            if (mode.equals(ProjectConstant.MODE_BUYBEHAVIOR_OMNI)) {
                configure.set(ProjectConstant.PARAM_LABEL_DATATYPE_RUNTIME, ProjectConstant.TYPE_US_WEB);
                int exitCode = ToolRunner.run(configure, new BehaviorJobClient(), otherArgs);
                logger.info("The job finish as {}", exitCode);

            } else if (mode.equals(ProjectConstant.MODE_BUYBEHAVIOR_STUBTRANS)) {
                configure.set(ProjectConstant.PARAM_LABEL_DATATYPE_RUNTIME, ProjectConstant.TYPE_STUB_TRANS);
                int exitCode = ToolRunner.run(configure, new BehaviorJobClient(), otherArgs);
                logger.info("The job finish as {}", exitCode);

            } else if (mode.equals(ProjectConstant.MODE_MAPFILE_EVENT)) {
                MapFileClient mapFileClient = new MapFileClient(configure);
                mapFileClient.createEventMapFile(endDateStr);
                logger.info("Event MapFile job is finished");

            } else if (mode.equals(ProjectConstant.MODE_MAPFILE_TICKET)) {
                MapFileClient mapFileClient = new MapFileClient(configure);
                mapFileClient.createTicketMapFile(startDateStr, endDateStr);
                logger.info("Transaction MapFile job is finished");

            } else if (mode.equals(ProjectConstant.MODE_MAPFILE_USER)) {
                MapFileClient mapFileClient = new MapFileClient(configure);
                mapFileClient.createUserMapFIle(endDateStr);
                logger.info("User MapFile job is finished");
            } else if (mode.equals(ProjectConstant.MODE_MAPFILE_USER_CONTACTS)) {
                MapFileClient mapFileClient = new MapFileClient(configure);
                mapFileClient.createUserContactsMapFIle(endDateStr);
                logger.info("User MapFile job is finished");
            } else if (mode.equals(ProjectConstant.MODE_MAPFILE_ZIPCODE_LOOKUP)) {
                MapFileClient mapFileClient = new MapFileClient(configure);
                mapFileClient.createZipcodeLookupMapFIle();
                logger.info("User MapFile job is finished");
            } else if (mode.equals(ProjectConstant.MODE_UNITTEST)) {
                Result result = JUnitCore.runClasses(UserMapFileFactorySmokeTest.class);
                for (Failure failure : result.getFailures()) {
                    logger.error(failure.toString());
                }
                logger.info("Test result is {}", result.wasSuccessful());

            } else if (mode.equals(ProjectConstant.MODE_STUBTRANS_FEED_USWEB) || mode.equals(ProjectConstant.MODE_STUBTRANS_FEED_ALL)) {

                /*
                Create the stub_trans feed data iteratively for each day
                 */
                configure.set(ProjectConstant.PARAM_LABEL_DATATYPE_RUNTIME, ProjectConstant.TYPE_STUB_TRANS);
                if (endDateStr == null || endDateStr.trim().length() == 0) {
                    endDateStr = startDateStr;
                }

                /*
                Make sure the mapfiles are ready to use
                 */
                logger.info("Check or create map file for events lookup");
                EventsMapFileFactory eventMapFile = new EventsMapFileFactory(configure);
                eventMapFile.loadMapFile();
                logger.info("Check or create map file for user lookup");
                UserMapFileFactory userMapFile = new UserMapFileFactory(configure);
                userMapFile.loadMapFile();
                logger.info("Check or create stub_trans feed data iteratively");
                while (!processStartDate.after(processEndDate)) {
                    String transactionDateStr = simpleFormatter.format(processStartDate);
                    String jobName = "SH_StubTrans_Feed_" + transactionDateStr;
                    configure.set(ProjectConstant.PARAM_LABEL_JOBNAME_RUNTIME, jobName);
                    configure.set(ProjectConstant.PARAM_LABEL_TRANSDATE_RUNTIME, transactionDateStr);
                    int exitCode = ToolRunner.run(configure, new StubTransClient(), otherArgs);
                    logger.info("Create stub_trans feed data on day {}. The job returns code as {}", transactionDateStr,
                            exitCode);
                    cal.setTime(processStartDate);
                    cal.add(Calendar.DATE, 1);
                    processStartDate = cal.getTime();
                }
                logger.info("StubTrans data creation job is finished");
            } else if (mode.equals(ProjectConstant.MODE_TRANS_RECORD_FEED)) {
                logger.info("Check or create map file for events lookup");
                EventsMapFileFactory eventMapFile = new EventsMapFileFactory(configure);
                eventMapFile.loadMapFile();
                logger.info("Check or create map file for user contact");
                UserContactsMapFileFactory userContactsMapfile = new UserContactsMapFileFactory(configure);
                userContactsMapfile.loadMapFile();

                while (!processStartDate.after(processEndDate)) {
                    String transactionDateStr = simpleFormatter.format(processStartDate);

                    Calendar runtimeCal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
                    runtimeCal.setTime(processStartDate);
                    runtimeCal.add(Calendar.DATE, 1);
                    String transactionEndDateStr = simpleFormatter.format(runtimeCal.getTime());

                    String jobName = "SH_Transaction_Record_" + transactionDateStr;
                    logger.debug("Set job name as {}", jobName);
                    configure.set(ProjectConstant.PARAM_LABEL_JOBNAME_RUNTIME, jobName);
                    configure.set(ProjectConstant.PARAM_LABEL_TRANSDATE_RUNTIME, transactionDateStr);
                    configure.set(ProjectConstant.PARAM_LABEL_TRANS_ENDDATE_RUNTIME, transactionEndDateStr);
                    int exitCode = ToolRunner.run(configure, new TransRecordJobClient(), otherArgs);
                    logger.debug("The job finish as {}", exitCode);
                    cal.setTime(processStartDate);
                    cal.add(Calendar.DATE, 1);
                    processStartDate = cal.getTime();
                }
                logger.info("Transaction Record job is finished");
            } else if (mode.equals(ProjectConstant.MODE_TRANS_GEOGRAPHY_FEED)) {
                while (!processStartDate.after(processEndDate)) {
                    String transactionDateStr = simpleFormatter.format(processStartDate);
                    String jobName = "SH_Transaction_Geography_" + transactionDateStr;
                    logger.debug("Set job name as {}", jobName);
                    configure.set(ProjectConstant.PARAM_LABEL_JOBNAME_RUNTIME, jobName);
                    configure.set(ProjectConstant.PARAM_LABEL_TRANSDATE_RUNTIME, transactionDateStr);
                    int exitCode = ToolRunner.run(configure, new TransactionGeographyJobClient(), otherArgs);
                    logger.debug("The job finish as {}", exitCode);
                    cal.setTime(processStartDate);
                    cal.add(Calendar.DATE, 1);
                    processStartDate = cal.getTime();
                }
                logger.info("Transaction Geography job is finish");
            } else if (mode.equals(ProjectConstant.MODE_TRANS_LOCALIZATION_FEED)) {
                while (!processStartDate.after(processEndDate)) {
                    String transactionDateStr = simpleFormatter.format(processStartDate);
                    String jobName = "SH_Transaction_Localization_" + transactionDateStr;
                    logger.debug("Set job name as {}", jobName);
                    configure.set(ProjectConstant.PARAM_LABEL_JOBNAME_RUNTIME, jobName);
                    configure.set(ProjectConstant.PARAM_LABEL_TRANSDATE_RUNTIME, transactionDateStr);
                    int exitCode = ToolRunner.run(configure, new TransactionLocalizationJobClient(), otherArgs);
                    logger.debug("The job finish as {}", exitCode);
                    cal.setTime(processStartDate);
                    cal.add(Calendar.DATE, 1);
                    processStartDate = cal.getTime();
                }
                logger.info("Transaction Localization job is finish");
            }
        } else {

            logger.error("Mode parameter is null.Program terminates.");

        }

    }
}
