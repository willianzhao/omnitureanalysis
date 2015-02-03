package org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transByTimezone.geoParse.mapreduce;

import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;
import org.willianzhao.omnitureanalysis.mapred.commons.misc.timezonehelper.GeoTimezone;
import org.willianzhao.omnitureanalysis.mapred.commons.misc.timezonehelper.TimezoneConverter;
import org.willianzhao.omnitureanalysis.mapred.commons.model.TransactionGeography;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.mapfile.GeonamesZipcodeMapFileFactory;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transaction.mapreduce.BrowseHistoryOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.TimeZone;

/**
 * Created by weilzhao on 8/21/14.
 */
public class GeoParseMapper extends Mapper<LongWritable, Text, Text, TransactionGeography> {
    Configuration conf = null;
    MapFile.Reader zipcodeMapfileReader;
    GeoTimezone gtz;
    TimezoneConverter tc;
	private static Logger logger = LoggerFactory.getLogger(GeoParseMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.conf = context.getConfiguration();
        if (zipcodeMapfileReader == null) {
            GeonamesZipcodeMapFileFactory zipcodeMapFile;
            try {
                zipcodeMapFile = new GeonamesZipcodeMapFileFactory(conf);
                zipcodeMapfileReader = zipcodeMapFile.getMapfileReader();
            } catch (Exception e) {
                //e.printStackTrace(logger.getStream(Level.ERROR));
            }
        }
        gtz = new GeoTimezone(conf);
        tc = new TimezoneConverter();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //Only use registered zip code to refer the longitude and lantitude
        String transactionID;
        String purchaseTime;
        String countryCode;
        String zipCode;
        String lantitude = "";
        String longitude = "";
        String state;
        String city;
        String searchKey;
        String timezoneStr = "";
        String localTime = "";
        String localDate = "";
        String localHourOfDay = "";
        String[] rawFields;
        double longitudeD;
        double lantitudeD;
        String s = value.toString();
        //Take care of the empty string in the record
        rawFields = s.split(ProjectConstant.DELIMITER,-1);
        if (rawFields.length == ProjectConstant.CONSTANT_TRANSREC_TOTAL_FIELDS_CNT) {
            transactionID = rawFields[ProjectConstant.CONSTANT_TRANSREC_TRANSACTION_ID_POS].trim();
            purchaseTime = rawFields[ProjectConstant.CONSTANT_TRANSREC_TIME_POS].trim();
            city = rawFields[ProjectConstant.CONSTANT_TRANSREC_R_CITY_POS].trim();
            state = rawFields[ProjectConstant.CONSTANT_TRANSREC_R_REGION_POS].trim();
            countryCode = rawFields[ProjectConstant.CONSTANT_TRANSREC_R_COUNTRY_CODE_POS].trim();
            zipCode = rawFields[ProjectConstant.CONSTANT_TRANSREC_R_ZIPCODE_POS].trim();
            searchKey = countryCode + ProjectConstant.CONSTANT_HITDATA_DELIMITER + zipCode;
            //Get the geography information from zipcode
            Text geo = new Text();
            Writable val = zipcodeMapfileReader.get(new Text(searchKey), geo);
            Text temp = val != null ? (Text) val : null;
            if (temp != null) {
            	//RM context.getCounter("Debug", "Found geo in zipcode lookup mapfile").increment(1);
                String geoStr = temp.toString();
                String[] valueList = geoStr.split(ProjectConstant.CONSTANT_HITDATA_DELIMITER);
                if (valueList.length == 2) {
                    lantitude = valueList[0].trim();
                    longitude = valueList[1].trim();
                    longitudeD = Double.parseDouble(longitude);
                    lantitudeD = Double.parseDouble(lantitude);
                    TimeZone tz = gtz.getTimeZone(lantitudeD, longitudeD);
//                timezoneStr=tz.getDisplayName(true,TimeZone.LONG);
                    timezoneStr = tz.getID();
                    //Create the default timezone of transaction records
                    TimeZone gmtTZ = TimeZone.getTimeZone("GMT");
                    try {
                        localTime = tc.convertTimeZoneString(gmtTZ, tz, purchaseTime);
                        localDate = tc.extractDate(localTime);
                        localHourOfDay = tc.extractHour(localTime);
                    } catch (ParseException e) {
                        localTime = "";
                      //RM context.getCounter("Debug", "Failed to parse timezone").increment(1);
                    }
                }
            } else {
            	//RM context.getCounter("Debug", "Failed to get timezone").increment(1);
            }
            Text outputKey = new Text(transactionID);
            TransactionGeography outputValue = new TransactionGeography(transactionID, purchaseTime);
            outputValue.setZipcodeCity(city);
            outputValue.setZipcodeState(state);
            outputValue.setZipcodeCountry(countryCode);
            outputValue.setZipcode(zipCode);
            outputValue.setZipcodeTimezone(timezoneStr);
            outputValue.setZipcodeLantitude(lantitude);
            outputValue.setZipcodeLongitude(longitude);
            outputValue.setLocalTime(localTime);
            outputValue.setLocalDate(localDate);
            outputValue.setLocalHourOfDay(localHourOfDay);
            context.write(outputKey, outputValue);
        } else {
        	//RM context.getCounter("Debug", "The input transaction record has incomplete fields").increment(1);
            logger.debug("Read the transaction record as '{}'", s);
        }
    }
}
