package org.willianzhao.omnitureanalysis.mapred.commons.misc;

/**
 * Created by willianzhao on 3/17/14.
 */
public class ProjectConstant {

    public final static String DELIMITER = " ~ ";
    public final static String DELIMITER_COMMA = ",";
    public final static String QUOTED_CHAR = "\"";
    public final static String CONSTANT_HITDATA_DELIMITER = "\t";
    public static final String CONTROLA_REC_DELIMITER = "\u0001";
    public final static String CONSTANT_NEWLINE = "\n";
    public final static String CONSTANT_TRUE = "1";
    public final static String CONSTANT_FALSE = "0";
    public final static String CONSTANT_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public final static String CONSTANT_DATE_MIN = "2000-01-01 00:00:00";
    public final static String CONSTANT_DATETIME_DELIMITER = " ";
    public final static String CONSTANT_DATE_DELIMITER = "-";
    public final static String CONSTANT_TIME_DELIMITER = ":";
    public final static String CONSTANT_PARAM_DATE_PATTERN = "yyyyMMdd";
    public final static int CONSTANT_PARAM_DATE_PATTERN_LENTH = 8;
    public final static int CONSTANT_GUID_LENGTH = 32;
    public final static int CONSTANT_MIN_ID_LENGTH = 3;
    public final static int CONSTANT_ASCII_DIGIT_START_POS = 47;
    public final static int CONSTANT_ASCII_DIGIT_END_POS = 58;
    public final static int CONSTANT_STUBTRANS_TICKETID_POS = 2;
    public final static int CONSTANT_STUBTRANS_EVENTID_POS = 33;
    public final static int CONSTANT_STUBTRANS_BUYERID_POS = 1;
    public final static int CONSTANT_STUBTRANS_SOURCEID_POS = 58;
    public final static int CONSTANT_STUBTRANS_BOBID_POS = 78;
    public final static int CONSTANT_STUBTRANS_TRANSACTIONID_POS = 0;
    public final static int CONSTANT_STUBTRANS_TRANSACTIONDATE_POS = 6;
    public final static int CONSTANT_USERS_GUIDID_POS = 28;
    public final static int CONSTANT_USERS_USERID_POS = 0;
    public final static int CONSTANT_USERS_DATEJOIN_POS = 3;
    public final static int CONSTANT_EVENTS_EVENTID_POS = 0;
    public final static int CONSTANT_EVENTS_GENREID_POS = 2;
    public final static int CONSTANT_EVENTS_DESC_POS = 4;
    public final static int CONSTANT_TICKETS_EVENTID_POS = 1;
    public final static int CONSTANT_TICKETS_TICKETID_POS = 0;
    public final static int CONSTANT_USERCONTACTS_OWNERID_POS = 1;
    public final static int CONSTANT_USERCONTACTS_ADDRCITY_POS = 7;
    public final static int CONSTANT_USERCONTACTS_ADDRSTATE_POS = 8;
    public final static int CONSTANT_USERCONTACTS_ADDRZIP_POS = 9;
    public final static int CONSTANT_USERCONTACTS_ADDRCOUNTRY_POS = 18;
    public final static int CONSTANT_USERCONTACTS_ACTIVE_POS = 15;
    public final static int CONSTANT_USERCONTACTS_DEFAULTFCONTACT_POS = 19;
    public final static int CONSTANT_GEONAMESZIP_COUNTRYCODE_POS = 0;
    public final static int CONSTANT_GEONAMESZIP_ZIPCODE_POS = 1;
    public final static int CONSTANT_GEONAMESZIP_LATI_POS = 9;
    public final static int CONSTANT_GEONAMESZIP_LONG_POS = 10;
    public final static int CONSTANT_GEONAMESZIP_TOTAL_FIELDS_CNT = 12;
    public final static int CONSTANT_USZIP_ZIPCODE_POS = 0;
    public final static int CONSTANT_USZIP_LATITUDE_POS = 3;
    public final static int CONSTANT_USZIP_LONGITUDE_POS = 4;
    public final static int CONSTANT_USZIP_TOTAL_FIELDS_CNT = 7;
    public final static int CONSTANT_TRANSREC_TRANSACTION_ID_POS = 3;
    public final static int CONSTANT_TRANSREC_TIME_POS = 1;
    public final static int CONSTANT_TRANSREC_R_CITY_POS = 14;
    public final static int CONSTANT_TRANSREC_R_REGION_POS = 15;
    public final static int CONSTANT_TRANSREC_R_COUNTRY_CODE_POS = 16;
    public final static int CONSTANT_TRANSREC_R_ZIPCODE_POS = 17;
    public final static int CONSTANT_TRANSREC_TOTAL_FIELDS_CNT = 18;
    public final static String CONSTANT_UNFOUND_FLAG = "-1";
    public final static String MODE_VISIT = "visit";
    public final static String MODE_VISITPATH = "visitpath";
    public final static String MODE_VISITOR = "visitor";
    public final static String MODE_BUYBEHAVIOR_OMNI = "checkoutbehavior";
    public final static String MODE_BUYBEHAVIOR_STUBTRANS = "stubtransbehavior";
    public final static String MODE_MAPFILE_EVENT = "createeventmapfile";
    public final static String MODE_MAPFILE_TICKET = "createticketmapfile";
    public final static String MODE_MAPFILE_USER = "createusermapfile";
    public final static String MODE_MAPFILE_USER_CONTACTS = "createusercontactsmapfile";
    public final static String MODE_MAPFILE_ZIPCODE_LOOKUP = "createzipcodelookupmapfile";
    public final static String MODE_STUBTRANS_FEED_USWEB = "createstubtransfeed";
    public final static String MODE_STUBTRANS_FEED_ALL = "createstubtransfeedall";
    public final static String MODE_TRANS_RECORD_FEED = "createtransrecordfeed";
    public final static String MODE_TRANS_LOCALIZATION_FEED = "createtranslocalizationfeed";
    public final static String MODE_TRANS_GEOGRAPHY_FEED = "createtransgeographyfeed";
    public final static String MODE_UNITTEST = "unittest";
    public final static String[] MODELIST = new String[]{MODE_BUYBEHAVIOR_OMNI, MODE_BUYBEHAVIOR_STUBTRANS, MODE_MAPFILE_EVENT, MODE_MAPFILE_TICKET, MODE_MAPFILE_USER,MODE_MAPFILE_USER_CONTACTS, MODE_STUBTRANS_FEED_USWEB, MODE_UNITTEST,MODE_TRANS_RECORD_FEED,MODE_TRANS_LOCALIZATION_FEED,MODE_TRANS_GEOGRAPHY_FEED,MODE_STUBTRANS_FEED_ALL,MODE_MAPFILE_ZIPCODE_LOOKUP};
    public final static String INPUT_DIRECTION = "INPUT";
    public final static String OUTPUT_DIRECTION = "OUTPUT";
    public final static String TYPE_US_WEB = "US_WEB";
    public final static String TYPE_US_MWEB = "US_MWEB";
    public final static String TYPE_UK_WEB = "UK_WEB";
    public final static String TYPE_UK_MWEB = "UK_MWEB";
    public final static String TYPE_IPAD_APPS = "IPAD_APPS";
    public final static String TYPE_MOBILE_APPS = "MOBILE_APPS";
    public final static String TYPE_IOS_APPS = "IOS_APPS";
    public final static String[] DATATYPELIST = new String[]{TYPE_US_WEB, TYPE_US_MWEB, TYPE_UK_WEB, TYPE_UK_MWEB, TYPE_IPAD_APPS, TYPE_MOBILE_APPS, TYPE_IOS_APPS};
    public final static String TYPE_STUB_TRANS = "STUB_TRANS";
    public final static String TYPE_ALL = "ALL";
    public final static String SOURCE_US_WEB = "US_WEB";
    public final static String SOURCE_US_MWEB = "US_MWEB";
    public final static String SOURCE_UK_WEB = "UK_WEB";
    public final static String SOURCE_UK_MWEB = "UK_MWEB";
    public final static String SOURCE_IPAD = "IPAD_APPS";
    public final static String SOURCE_IOS = "IOS"; 
    public final static String SOURCE_MOBILE = "MOBILE_APPS";
    public final static String SOURCE_STUB_TRANS = "STUB_TRANS";
    public final static String SOURCE_OMNITURE = "OMNITURE";
    public final static String SOURCE_OTHERS = "OTHERS";
    public final static String CONF_SH = "stubhub.conf";
    public final static String CONF_HITDATA = "hit_data.conf";
    public final static String CONF_COLUMNHEADER = "column_headers.tsv";
    public final static String CONF_LABEL_BEHAVIORPATH = "BEHAVIOR_PATH_ROOT";
    public final static String CONF_LABEL_LOOKUPSERVER = "LOOKUP_SERVER";
    public final static String CONF_LABEL_GENREFILE = "GENRE_FILE";
    public final static String CONF_LABEL_EVENTFILE = "EVENT_FILE";
    public final static String PARAM_LABEL_MODE = "MODE";
    public final static String PARAM_LABEL_DATATYPE = "DATATYPE";
    public final static String PARAM_LABEL_STARTDATE = "STARTDATE";
    public final static String PARAM_LABEL_ENDDATE = "ENDDATE";
    public final static String PARAM_LABEL_DURATION = "DURATION";
    public final static String PARAM_LABEL_DEBUG = "DEBUG";
    public final static String PARAM_LABEL_DATATYPE_RUNTIME = "DATATYPERUNTIME";
    public final static String PARAM_LABEL_TRANSDATE_RUNTIME = "TRANSDATERUNTIME";
    public final static String PARAM_LABEL_TRANS_ENDDATE_RUNTIME = "TRANSENDDATERUNTIME";
    public final static String PARAM_LABEL_JOBNAME_RUNTIME = "JOBNAMERUNTIME";
    public final static String TASK_USCHECKOUT_BROWSEHISTORY = "US_CHECKOUT_BROWSEHISTORY";
    public final static String TASK_USSTUBTRANS_BROWSEHISTORY = "USWWW_STUBTRANS_BROWSEHISTORY";
    public final static String TASK_UKCHECKOUT_BROWSEHISTORY = "UK_CHECKOUT_BROWSEHISTORY";
    public final static String TASK_UKSTUBTRANS_BROWSEHISTORY = "UKWWW_STUBTRANS_BROWSEHISTORY";
    public final static String TASK_MAPFILE_EVENTS = "EVENTS_MAPFILE";
    public final static String TASK_MAPFILE_TICKETS = "TICKETS_MAPFILE";
    public final static String TASK_MAPFILE_USERS = "USERS_MAPFILE";
    public final static String TASK_MAPFILE_USER_CONTACTS = "USER_CONTACTS_MAPFILE";
    public final static String TASK_MAPFILE_GEONAMES_ZIPCODE = "ZIPCODE_LOOKUP_MAPFILE";
    public final static String TASK_CHECKOUT_BROWSEDETAIL = "CheckoutDetail";
    public final static String PAGENAME_PC_CHECKOUTPAGE = "Checkout - Thank you";
    public final static String PAGENAME_MOBILE_CHECKOUTPAGE = "mCheckout - Thank You";
    public final static String ACTION_CHECKOUT = "CHECKOUT";
    public final static String ACTION_BROWSE = "BROWSE";
    public final static String ACTION_NONEED = "NONEED";
    public static final String PATH_EVENTS_MAPFILE = "EVENT_FILE_ROOT";
    public static final String PATH_TICKETS_MAPFILE = "TICKET_FILE_ROOT";
    public static final String PATH_USERS_MAPFILE = "USER_FILE_ROOT";
    public static final String PATH_GENRENODES_FILE = "GENRE_FILE";
    public static final String PATH_STUBTRANS_FILE = "STUBTRANS_FILE_ROOT";
    public static final String PATH_USER_CONTACTS_FILE = "USER_CONTACTS_FILE_ROOT";
    public static final String PATH_STUBTRANS_USWEB = "stub_trans_usweb_utc";
    public static final String PATH_STUBTRANS_ALL = "stub_trans_all_utc";
    public static final String PATH_ALL_TRANS_RECORD = "all_trans_record";
    public static final String PATH_ALL_TRANS_LOCALIZATION = "all_trans_localization";
    public static final String PATH_ALL_TRANS_GEOGRAPHY = "all_trans_geography";
    public static final String PATH_MAPFILE = "mapfile";
    public static final String PATH_MAPFILE_LASTCHNAGE = "LAST_MODIFIED";
    public static final String PATH_RESOURCE_ROOT = "SH_HDFS_RESOURCE_ROOT";
    public static final String PATH_TZWORLDMP_PREFIX = "TZWORLDMP";
    public static final String PATH_ALLCOUNTRY_FILE = "ALLCOUNTRY_FILE";
    public static final String PATH_USZIPCODE_FILE = "USZIPCODE_FILE";
    public static final String LOOKAHEAD_DAYS = "0";
    public static final int STUBTRANS_LOOKAHEAD_DAYS = -2;
    public static final int STUBTRANS_LOOKAROUND_DAYS = 100;
    public static final String VALID_REC_THRESHHOLD = "380";
    public static final int MAPFILE_UPBOUND_DAYS = 365;
    public static final int MAPFILE_MAPDB_NODES = 120;
    public static final String PATTERN_PREFIX_TICKETID_STR = "ticket_id=";
    public static final int PATTERN_PREFIX_TICKETID_STR_LENTTH = 9;
    public static final int PATTERN_TICKETID_STR_MAXLENTTH = 11;
    public static final int PATTERN_TICKETID_STR_MINLENTTH = 7;
    public static final int PATTERN_EVENTID_STR_MAXLENTTH = 10;
    public static final int PATTERN_GENREID_STR_MAXLENTTH = 10;
    public static final String GENRE_CUT_FINAL_CONF_PREFIX = "GCF_";
    public static final int GENRE_CUT_FINAL_FIELD_CNT = 21;
    public static final String TOKEN_MAPFILE_USER_CONTACTS="user_contacts";
    public static final String TOKEN_MAPFILE_USERS="users";
    public static final String TOKEN_MAPFILE_EVENTS="events";
    public static final String TOKEN_MAPFILE_TICKETS="tickets";
    public static final String TOKEN_MAPFILE_GEONAMES_ZIPCODE="zipcodelookup";
    public static final String FORWARD_SLASH="/";

    public static String getUsage() {
        return "usage : please check project README";
    }
}
