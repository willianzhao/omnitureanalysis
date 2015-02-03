package org.willianzhao.omnitureanalysis.mapred.commons.misc.timezonehelper;

import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;

import java.text.ParseException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * This class is to convert time string from source timezone to target timezone
 * Created by weilzhao on 9/3/14.
 */
public class TimezoneConverter {

    /*
    Convert the datetime string between different timezone

    @Param fromTimezoneStr
    @Param toTimezoneStr
    @Param inTimeStr
     */
    public String convertTimeZoneString(String fromTimezoneStr, String toTimezoneStr, String inTimeStr) throws ParseException {
        //The datetime string pattern is following ProjectConstant.CONSTANT_DATE_PATTERN : "yyyy-MM-dd HH:mm:ss".
        String[] dateTimePair = inTimeStr.split(ProjectConstant.CONSTANT_DATETIME_DELIMITER);
        if (dateTimePair.length != 2) {
            throw new ParseException("Unrecognized DateTime String", 0);
        }
        //Parse the date part
        String dateStr = dateTimePair[0];
        String[] dateFields = dateStr.split(ProjectConstant.CONSTANT_DATE_DELIMITER);
        if (dateFields.length != 3) {
            throw new ParseException("Unrecognized Date String", 1);
        }
        String yearStr = dateFields[0];
        String monthStr = dateFields[1];
        String dayStr = dateFields[2];
        int year = Integer.parseInt(yearStr);
        int month = Integer.parseInt(monthStr);
        int day = Integer.parseInt(dayStr);
        //Parse the time part
        String timeStr = dateTimePair[1];
        String[] timeFields = timeStr.split(ProjectConstant.CONSTANT_TIME_DELIMITER);
        if (timeFields.length != 3) {
            throw new ParseException("Unrecognized Time String", 2);
        }
        String hourStr = timeFields[0];
        String minStr = timeFields[1];
        String secStr = timeFields[2];
        int hour = Integer.parseInt(hourStr);
        int minute = Integer.parseInt(minStr);
        int second = Integer.parseInt(secStr);
        //The source calendar
        Calendar sourceCalendar = GregorianCalendar.getInstance(TimeZone.getTimeZone(fromTimezoneStr));
        sourceCalendar.set(Calendar.YEAR, year);
        //Month is starting from Jan which is 0
        sourceCalendar.set(Calendar.MONTH, month - 1);
        sourceCalendar.set(Calendar.DAY_OF_MONTH, day);
        sourceCalendar.set(Calendar.HOUR_OF_DAY, hour);
        sourceCalendar.set(Calendar.MINUTE, minute);
        sourceCalendar.set(Calendar.SECOND, second);
        //The target calendar
        Calendar targetCalendar = GregorianCalendar.getInstance(TimeZone.getTimeZone(toTimezoneStr));
        targetCalendar.setTimeInMillis(sourceCalendar.getTimeInMillis());
        String convertedDatetimeStr = String.format("%04d-%02d-%02d %02d:%02d:%02d", targetCalendar.get(Calendar.YEAR), targetCalendar.get(Calendar.MONTH) + 1, targetCalendar.get(Calendar.DAY_OF_MONTH), targetCalendar.get(Calendar.HOUR_OF_DAY), targetCalendar.get(Calendar.MINUTE), targetCalendar.get(Calendar.SECOND));
        return convertedDatetimeStr;
    }

    /*
   Convert the datetime string between different timezone

   @Param fromTimezone
   @Param toTimezone
   @Param inTimeStr
    */
    public String convertTimeZoneString(TimeZone fromTimezone, TimeZone toTimezone, String inTimeStr) throws ParseException {
        //The datetime string pattern is following ProjectConstant.CONSTANT_DATE_PATTERN : "yyyy-MM-dd HH:mm:ss".
        String[] dateTimePair = inTimeStr.split(ProjectConstant.CONSTANT_DATETIME_DELIMITER);
        if (dateTimePair.length != 2) {
            throw new ParseException("Unrecognized DateTime String", 0);
        }
        //Parse the date part
        String dateStr = dateTimePair[0];
        String[] dateFields = dateStr.split(ProjectConstant.CONSTANT_DATE_DELIMITER);
        if (dateFields.length != 3) {
            throw new ParseException("Unrecognized Date String", 1);
        }
        String yearStr = dateFields[0];
        String monthStr = dateFields[1];
        String dayStr = dateFields[2];
        int year = Integer.parseInt(yearStr);
        int month = Integer.parseInt(monthStr);
        int day = Integer.parseInt(dayStr);
        //Parse the time part
        String timeStr = dateTimePair[1];
        String[] timeFields = timeStr.split(ProjectConstant.CONSTANT_TIME_DELIMITER);
        if (timeFields.length != 3) {
            throw new ParseException("Unrecognized Time String", 2);
        }
        String hourStr = timeFields[0];
        String minStr = timeFields[1];
        String secStr = timeFields[2];
        int hour = Integer.parseInt(hourStr);
        int minute = Integer.parseInt(minStr);
        int second = Integer.parseInt(secStr);
        //The source calendar
        Calendar sourceCalendar = GregorianCalendar.getInstance(fromTimezone);
        sourceCalendar.set(Calendar.YEAR, year);
        //Month is starting from Jan which is 0
        sourceCalendar.set(Calendar.MONTH, month - 1);
        sourceCalendar.set(Calendar.DAY_OF_MONTH, day);
        sourceCalendar.set(Calendar.HOUR_OF_DAY, hour);
        sourceCalendar.set(Calendar.MINUTE, minute);
        sourceCalendar.set(Calendar.SECOND, second);
        //The target calendar
        Calendar targetCalendar = GregorianCalendar.getInstance(toTimezone);
        targetCalendar.setTimeInMillis(sourceCalendar.getTimeInMillis());
        String convertedDatetimeStr = String.format("%04d-%02d-%02d %02d:%02d:%02d", targetCalendar.get(Calendar.YEAR), targetCalendar.get(Calendar.MONTH) + 1, targetCalendar.get(Calendar.DAY_OF_MONTH), targetCalendar.get(Calendar.HOUR_OF_DAY), targetCalendar.get(Calendar.MINUTE), targetCalendar.get(Calendar.SECOND));
        return convertedDatetimeStr;
    }

    public String extractHour(String inTimeStr) throws ParseException {
        //The datetime string pattern is following ProjectConstant.CONSTANT_DATE_PATTERN : "yyyy-MM-dd HH:mm:ss".
        String[] dateTimePair = inTimeStr.split(ProjectConstant.CONSTANT_DATETIME_DELIMITER);
        if (dateTimePair.length != 2) {
            throw new ParseException("Unrecognized DateTime String", 0);
        }
        //Parse the time part
        String timeStr = dateTimePair[1];
        String[] timeFields = timeStr.split(ProjectConstant.CONSTANT_TIME_DELIMITER);
        if (timeFields.length != 3) {
            throw new ParseException("Unrecognized Time String", 2);
        }
        String hourStr = timeFields[0];
        return hourStr;
    }

    public String extractDate(String inTimeStr) throws ParseException {
        //The datetime string pattern is following ProjectConstant.CONSTANT_DATE_PATTERN : "yyyy-MM-dd HH:mm:ss".
        String[] dateTimePair = inTimeStr.split(ProjectConstant.CONSTANT_DATETIME_DELIMITER);
        if (dateTimePair.length != 2) {
            throw new ParseException("Unrecognized DateTime String", 0);
        }
        //Parse the time part
        String dateStr = dateTimePair[0];
        return dateStr;
    }

}
