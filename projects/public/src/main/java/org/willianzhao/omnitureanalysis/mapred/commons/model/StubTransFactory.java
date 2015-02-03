package org.willianzhao.omnitureanalysis.mapred.commons.model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.willianzhao.omnitureanalysis.mapred.commons.dm.TransactionIdentifier;
import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;

/**
 * Created by willianzhao on 6/10/14.
 */
public class StubTransFactory {

    boolean getUSWEBonly = false;
    boolean getAllPlatform = false;
    Configuration conf;
    Context context;
    MapFile.Reader eventMapFileReader;
    MapFile.Reader userMapFileReader;
    String currentTransDateStr;
    SimpleDateFormat simpleFormatter;
    Calendar currentTransDateCal;
    Calendar nextTransDateCal;
    Calendar transDateCal;
	private static Logger logger = LoggerFactory.getLogger(StubTransFactory.class);

    public StubTransFactory(String mode, Context context, Configuration conf, MapFile.Reader eventMapFileReader, MapFile.Reader userFileReader) throws ParseException {
        if (mode.equals(ProjectConstant.MODE_STUBTRANS_FEED_USWEB)) {
            getUSWEBonly = true;
        } else if (mode.equals(ProjectConstant.MODE_STUBTRANS_FEED_ALL)) {
            getAllPlatform = true;
        }
        this.conf = conf;
        this.eventMapFileReader = eventMapFileReader;
        this.userMapFileReader = userFileReader;
        this.context = context;
        simpleFormatter = new SimpleDateFormat(ProjectConstant.CONSTANT_DATE_PATTERN);
        transDateCal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        currentTransDateCal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        SimpleDateFormat paramDateFormat = new SimpleDateFormat(ProjectConstant.CONSTANT_PARAM_DATE_PATTERN);
        currentTransDateStr = conf.get(ProjectConstant.PARAM_LABEL_TRANSDATE_RUNTIME);
        Date currentTransDate = paramDateFormat.parse(currentTransDateStr);
        currentTransDateCal.setTime(currentTransDate);
        //Clone a new instance
        nextTransDateCal = (Calendar)currentTransDateCal.clone();
        nextTransDateCal.add(Calendar.DATE, 1);
    }

    public VisitStep getVisitStep(String[] rawFields) {
        boolean proceedThis = false;
        TransactionIdentifier transIden = new TransactionIdentifier(context, conf, rawFields, eventMapFileReader, userMapFileReader);
        String orderSourceID = transIden.geteOrderSourceID();
        String bobID = transIden.geteBOBID();
        VisitStep step = new VisitStep();
        if (getUSWEBonly) {
            if (orderSourceID.equals("1") && bobID.equals("1")) {
                proceedThis = true;
            }
        } else if (getAllPlatform) {
            proceedThis = true;
        }
        if (proceedThis) {
            String actionType = ProjectConstant.ACTION_CHECKOUT;
            String dataSource = ProjectConstant.SOURCE_STUB_TRANS;
            String transDateStr;
            transDateStr = transIden.getTransDateUTC();
            if (withinTransactionDate(transDateStr)) {
                String userID;
                String eventID;
                String genreID;
                String ticketID;
                String transactionID;
                String buyerRegisterTime;
                String ecommUserID;
                userID = transIden.getUserID();
                eventID = transIden.getEventID();
                ticketID = transIden.getTicketID();
                transactionID = transIden.geteTransactionID();
                genreID = transIden.getGenreID();
                ecommUserID = transIden.geteCommUserID();
                buyerRegisterTime = transIden.getUserRegisteTime();
                step.setUserID(userID);
                step.setActionType(actionType);
                step.setDataSource(dataSource);
                step.setEventID(eventID);
                step.setGenreID(genreID);
                step.setTicketID(ticketID);
                step.setVisitTime(transDateStr);
                step.setTransactionID(transactionID);
                step.setBuyerID(ecommUserID);
                step.setBuyerRegisterTime(buyerRegisterTime);
              //RM context.getCounter("Debug", "Duped transaction (on " + currentTransDateStr + " UTC) #").increment(1);

            } else {
            	//RM context.getCounter("Debug", "Abandoned transaction #").increment(1);
                step.setActionType(ProjectConstant.ACTION_NONEED);

            }
        } else {

            /*
            for the transaction not belonging to the scope
             */
            step.setActionType(ProjectConstant.ACTION_NONEED);
        }
        return step;
    }

    boolean withinTransactionDate(String transDatePDTStr) {
        try {
            Date transDate = simpleFormatter.parse(transDatePDTStr);
            transDateCal.setTime(transDate);
            return (!transDateCal.before(currentTransDateCal)) && transDateCal.before(nextTransDateCal);
        } catch (ParseException pe) {
            return false;
        }
    }
}
