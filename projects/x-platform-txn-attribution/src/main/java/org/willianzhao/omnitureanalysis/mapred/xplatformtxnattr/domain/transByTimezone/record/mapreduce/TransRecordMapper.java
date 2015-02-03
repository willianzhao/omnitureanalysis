package org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transByTimezone.record.mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.willianzhao.omnitureanalysis.mapred.commons.dm.PageIdentifier;
import org.willianzhao.omnitureanalysis.mapred.commons.dm.TimeIdentifier;
import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;
import org.willianzhao.omnitureanalysis.mapred.commons.misc.timezonehelper.TimezoneConverter;
import org.willianzhao.omnitureanalysis.mapred.commons.model.TransactionRecord;
import org.willianzhao.omnitureanalysis.mapred.commons.model.TransactionStudyFactory;

/**
 * Created by weilzhao on 8/21/14.
 */
public class TransRecordMapper extends Mapper<LongWritable, Text, Text, TransactionRecord> {
    int threshhold;
    int position;
    final String fromTimeZone = "America/Los_Angeles";
    final String toTimeZone = "GMT";
    Calendar transStartDateUTCCal;
    Calendar transEndDateUTCCal;
    String exclude_hit = "";
    Configuration conf = null;
    TimezoneConverter tc;
    TransactionStudyFactory transactionStudyFactory;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.conf = context.getConfiguration();
        threshhold = Integer.parseInt(ProjectConstant.VALID_REC_THRESHHOLD);
        Path filePath = ((FileSplit) context.getInputSplit()).getPath();
        try {
            transactionStudyFactory = new TransactionStudyFactory(conf, filePath);
        } catch (Exception e) {
            transactionStudyFactory = null;
        }
        String transStartDateUTCStr = conf.get(ProjectConstant.PARAM_LABEL_TRANSDATE_RUNTIME);
        String transEndDateUTCStr = conf.get(ProjectConstant.PARAM_LABEL_TRANS_ENDDATE_RUNTIME);
        SimpleDateFormat paramDateFormater = new SimpleDateFormat(ProjectConstant.CONSTANT_PARAM_DATE_PATTERN);
        transStartDateUTCCal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        transEndDateUTCCal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        Date transDateGMT = null;
        try {
            transDateGMT = paramDateFormater.parse(transStartDateUTCStr);
            transStartDateUTCCal.setTime(transDateGMT);
            transDateGMT = paramDateFormater.parse(transEndDateUTCStr);
            transEndDateUTCCal.setTime(transDateGMT);
        } catch (ParseException e) {
            //e.printStackTrace(logger.getStream(Level.ERROR));
        }
        tc = new TimezoneConverter();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] rawFields;
        if (transactionStudyFactory != null) {
            String dataSource = transactionStudyFactory.getDataSource();
            if (dataSource.equals(ProjectConstant.SOURCE_STUB_TRANS)) {
                rawFields = value.toString().split(ProjectConstant.DELIMITER);
                TransactionRecord tRecord = transactionStudyFactory.readFromStubTrans(rawFields);
                Text transactionID = new Text(tRecord.getTransactionID());
                context.write(transactionID, tRecord);
              //RM context.getCounter("Debug", "Output from stub_trans ").increment(1);
            } else {
                rawFields = value.toString().split(ProjectConstant.CONSTANT_HITDATA_DELIMITER);
                int fieldsLength = rawFields.length;
                if (fieldsLength > threshhold) {
                    //Transaction id should not be empty
                    PageIdentifier pIden = new PageIdentifier(conf, rawFields);
                    String transID = pIden.getTransactionID();
                    if (transID.length() > ProjectConstant.CONSTANT_MIN_ID_LENGTH) {
                        //Consider the timezone
                        //Because Omniture data is PDT/PST timezone, so we need to convert it to UTC to work with Stub_Trans data
                        TimeIdentifier tIden = new TimeIdentifier(conf, rawFields);
                        String visitTime = tIden.getVisitTime();
                        try {
                            String utcVisitTime = tc.convertTimeZoneString(fromTimeZone, toTimeZone, visitTime);
                            Calendar transVisitTimeUTCCal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
                            SimpleDateFormat dateFormater = new SimpleDateFormat(ProjectConstant.CONSTANT_DATE_PATTERN);
                            Date visitDateGMT = dateFormater.parse(utcVisitTime);
                            transVisitTimeUTCCal.setTime(visitDateGMT);
                            if (transVisitTimeUTCCal.after(transStartDateUTCCal) && transVisitTimeUTCCal.before(transEndDateUTCCal)) {
                                //Start to process the qualified records
                                TransactionRecord tRecord = transactionStudyFactory.readFromOmniture(rawFields);
                                Text transactionID = new Text(transID);
                                context.write(transactionID, tRecord);
                              //RM context.getCounter("Debug", "Output from clickstream ").increment(1);
                            } else {
                            	//RM context.getCounter("Debug", "Clickstream records are outside of time window ").increment(1);
                            }
                        } catch (ParseException e) {
                            //skip this record
                        	//RM context.getCounter("Debug", "Failed to parse the visit time ").increment(1);
                        }
                    }
                } else {
                	//RM context.getCounter("Debug", "the omniture record is incomplete ").increment(1);
                }
            }
        } else {
        	//RM context.getCounter("Debug", "Records fail to output due to null transactionStudyFactory").increment(1);
        }
    }
}
