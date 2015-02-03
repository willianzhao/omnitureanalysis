package org.willianzhao.omnitureanalysis.mapred.commons.dm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;
import org.willianzhao.omnitureanalysis.mapred.commons.misc.timezonehelper.TimezoneConverter;

/**
 * Created by willianzhao on 5/24/14.
 * This class is working with STUB_TRANS
 */
public class TransactionIdentifier extends SmartIdentifier {

    final String fromTimeZone = "America/Los_Angeles";
    final String toTimeZone = "GMT";
    String eCommUserID = null;
    String guid = null;
    String userRegisteTime = null;
    String eventID = null;
    Mapper.Context context;
    MapFile.Reader eventMapFileReader;
    MapFile.Reader userMapFileReader;

    public TransactionIdentifier(Mapper.Context context, Configuration config, String[] inputFields, MapFile.Reader eventMapFileReader, MapFile.Reader userFileReader) {
        super(config, inputFields);
        this.eventMapFileReader = eventMapFileReader;
        this.userMapFileReader = userFileReader;
        this.context = context;
    }

    public String getTicketID() {
        int position = ProjectConstant.CONSTANT_STUBTRANS_TICKETID_POS;
        try {
            String ticketID = rawFields[position].trim();
            if (ticketID.length() > 0) {
                return ticketID;
            } else {
                return "";
            }
        } catch (Exception aie) {
            return "";
        }
    }

    public String getEventID() {
        if (eventID != null) {
            return eventID;
        }
        int position = ProjectConstant.CONSTANT_STUBTRANS_EVENTID_POS;
        try {
            eventID = rawFields[position].trim();
        } catch (Exception aie) {
            eventID = "";
        }
        return eventID;
    }

    public String geteCommUserID() {
        if (eCommUserID != null) {
            return eCommUserID;
        }
        // buyer_id
        int position = ProjectConstant.CONSTANT_STUBTRANS_BUYERID_POS;
        try {
            eCommUserID = rawFields[position].trim();

        } catch (Exception aie) {
            eCommUserID = "";
        }
        return eCommUserID;
    }

    public String geteOrderSourceID() {
        // tid
        int position = ProjectConstant.CONSTANT_STUBTRANS_SOURCEID_POS;
        try {
            String orderSourceID = rawFields[position].trim();
            if (orderSourceID.length() > 0) {
                return orderSourceID;
            } else {
                return "";
            }
        } catch (Exception aie) {
            return "";
        }
    }

    public String geteTransactionID() {
        // tid
        int position = ProjectConstant.CONSTANT_STUBTRANS_TRANSACTIONID_POS;
        try {
            String transID = rawFields[position].trim();
            if (transID.length() > 0) {
                return transID;
            } else {
                return "";
            }
        } catch (Exception aie) {
            return "";
        }
    }

    public String geteBOBID() {
        // tid
        int position = ProjectConstant.CONSTANT_STUBTRANS_BOBID_POS;
        try {
            String bobid = rawFields[position].trim();
            if (bobid.length() > 0) {
                return bobid;
            } else {
                return "";
            }
        } catch (Exception aie) {
            return "";
        }
    }

    public String getTransDateUTC() {
        // date_added
        int position = ProjectConstant.CONSTANT_STUBTRANS_TRANSACTIONDATE_POS;
        try {
            String transDateStr = rawFields[position].trim();
            if (transDateStr.length() > 0) {
                return transDateStr;
            } else {
                return "";
            }
        } catch (Exception aie) {
            return "";
        }
    }

    public String getTransDatePDT() {
        // date_added
        int position = ProjectConstant.CONSTANT_STUBTRANS_TRANSACTIONDATE_POS;
        try {
            String transDateStr = rawFields[position].trim();
            TimezoneConverter tc = new TimezoneConverter();
            if (transDateStr.length() > 0) {
                return tc.convertTimeZoneString(fromTimeZone, toTimeZone, transDateStr);
            } else {
                return "";
            }
        } catch (Exception aie) {
            return "";
        }
    }

    public String getGUID() {
        if (guid == null) {
            loadBuyerInfo();
        }
        if (guid == null) {
            //give a default value
            guid = "";
        }
        return guid;
    }

    public String getUserRegisteTime() {
        if (userRegisteTime == null) {
            loadBuyerInfo();
        }
        if (userRegisteTime == null) {
            userRegisteTime = "";
        }
        return userRegisteTime;
    }

    public void loadBuyerInfo() {
        if (eCommUserID == null) {
            geteCommUserID();
        }
        if (eCommUserID != null) {
            try {
                LongWritable ecommUserIDKey = new LongWritable(Long.parseLong(eCommUserID));
                Text userValue = new Text();
                Writable val = userMapFileReader.get(ecommUserIDKey, userValue);
                Text temp = val != null ? (Text) val : null;
                if (temp != null) {
                	//RM context.getCounter("Debug", "Lookup user Mapfile times").increment(1);
                    String tempValue = temp.toString();

                    /*
                    The user value has below format:

                    [user register time],[guid #],[guid],[guid]...
                     */
                    String[] userInfoList = tempValue.split(ProjectConstant.DELIMITER);
                    if (userInfoList.length >= 2) {
                    	//RM context.getCounter("Debug", "Find valid user records").increment(1);
                        userRegisteTime = userInfoList[0];
                        String guidCnt = userInfoList[1];
                        if (!guidCnt.equals("0")) {
                            /*
                            Only get the first the guid
                             */
                            guid = userInfoList[2];
                        } else {
                        	//RM context.getCounter("Debug", "Find user records with empty guid").increment(1);
                        }
                    } else {
                    	//RM context.getCounter("Debug", "Find invalid user records").increment(1);

                    }
                }
            } catch (Exception e) {
                // do nothing
            }
        }
        if (userRegisteTime == null) {
            userRegisteTime = "";
        }
        if (guid == null) {
            guid = "";
        }
    }

    public String getUserID() {

        /*
        The user ID means the unique identification code for user. It may come from guid or ecomm user id.
         */
        String localGuid = this.getGUID();
        if (localGuid.trim().length() > 0) {
            return localGuid;
        } else {
            String ecommUserID = this.geteCommUserID();
            if (ecommUserID != null && ecommUserID.trim().length() > 0) {
                return ecommUserID;
            } else {
                return "";
            }
        }
    }

    public String getGenreID() {
        String genreID = "";
        if (eventID == null) {
            getEventID();
        }
        if (eventID != null) {
            genreID= getGenreIDFromMapFile(eventID);
        }
        return genreID;
    }

    public String getGenreIDFromMapFile(String eventID1) {
        String valueStr;
        String genreID = "";
        if (eventID1 != null) {
            try {
                LongWritable eventKey = new LongWritable(Long.parseLong(eventID1));
                Text value = new Text();
                Writable val = eventMapFileReader.get(eventKey, value);
                Text temp = val != null ? (Text) val : null;
                if (temp != null) {
                    valueStr = temp.toString();
                    String[] valueList = valueStr.split(ProjectConstant.DELIMITER);
                    if (valueList.length == 2) {
                        genreID = valueList[0];
                    } else {
                        genreID = "";
                    }
                } else {
                    genreID = "";
                }
            } catch (Exception e) {
                genreID = "";
            }

        }
        return genreID;
    }

    public String getEventDescFromMapFile(String eventID1) {
        String valueStr;
        String eventDesc = "";
        if (eventID1 != null) {
            try {
                LongWritable eventKey = new LongWritable(Long.parseLong(eventID1));
                Text value = new Text();
                Writable val = eventMapFileReader.get(eventKey, value);
                Text temp = val != null ? (Text) val : null;
                if (temp != null) {
                    valueStr = temp.toString();
                    String[] valueList = valueStr.split(ProjectConstant.DELIMITER);
                    if (valueList.length == 2) {
                        eventDesc = valueList[1];
                    } else {
                        eventDesc = "";
                    }
                } else {
                    eventDesc = "";
                }
            } catch (Exception e) {
                eventDesc = "";
            }

        }
        return eventDesc;
    }
}
