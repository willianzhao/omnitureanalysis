package org.willianzhao.omnitureanalysis.mapred.commons.dm;

import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by willianzhao on 3/24/14.
 */
public class UserIdentifier extends SmartIdentifier {

    String userID = null;
    String ecommUserID = null;
    String guid = null;
    String mappedUserID = null;

	private static Logger logger = LoggerFactory.getLogger(GenreIdentifier.class);

    public UserIdentifier(Configuration config, String[] inputFields) {
        super(config, inputFields);
    }

    public String getEComUserID() {
        if (ecommUserID == null) {
            int position = 0;
            try {
                // post_evar27
                position = Integer.parseInt(conf.get("POST_EVAR27"));
                String oracleUserID = rawFields[position].trim();
                int length = oracleUserID.length();
                if (length > 0 && length < 32) {
                    //TODO: here it may need database user table validation
                    ecommUserID = oracleUserID;
                } else if (length == 32) {
                    guid = oracleUserID;

                }
                if (ecommUserID == null) {
                    position = Integer.parseInt(conf.get("EVAR27"));
                    oracleUserID = rawFields[position].trim();
                    length = oracleUserID.length();
                    if (length > 0 && length < 32) {
                        //TODO: here it may need database user table validation
                        ecommUserID = oracleUserID;
                    } else if (length == 32) {
                        guid = oracleUserID;
                    } else {
                        ecommUserID = "";
                    }
                }
            } catch (Exception aie) {
                ecommUserID = "";
            }
        }
        return ecommUserID;
    }

    public String getGUID() {
        if (guid == null) {
            int position = 0;
            try {
                // post_evar66
                position = Integer.parseInt(conf.get("POST_EVAR66"));
                String localGuid = rawFields[position].trim();
                if (localGuid.length() == 32) {
                    guid = localGuid;
                } else {
                    position = Integer.parseInt(conf.get("EVAR66"));
                    localGuid = rawFields[position].trim();
                    if (localGuid.length() == 32) {
                        guid = localGuid;
                    } else {
                        guid = "";
                    }
                }
            } catch (Exception aie) {
                guid = "";
            }

        }
        return guid;
    }

    public String getUserID() {

        /*
        The user ID means the unique identification code for user. It may come from guid or ecomm user id.
         */
        String localGuid = this.getGUID();
        if (localGuid.trim().length() > 0) {
            return localGuid;
        } else {
            String ecommUserID = this.getEComUserID();
            if (guid.trim().length() > 0) {
                return guid;
            } else if (ecommUserID != null && ecommUserID.trim().length() > 0) {
                return ecommUserID;
            } else {
                return "";
            }
        }
    }

    public String getMappedUserID() {

        /*
        The mapped user id is for un-authenticated user who can be guessed as the proper user
         */
        //TODO : Impl
        // logger.trace("Not implement the mapped user ID. Return empty ID");
        return "";
    }

    public String getUserAgent() {
        int position = 0;
        try {
            // user_agent
            position = Integer.parseInt(conf.get("USER_AGENT"));
            String userAgent = rawFields[position].trim();
            if (userAgent.length() > 0) {
                //TODO: here it may need format the user agent string
                return userAgent;
            } else {
                return "";
            }
        } catch (Exception aie) {
            logger.error("Fail to get user agent field. Report Exception");
            return "";
        }
    }

    public boolean isAuthenUser() {
        if (dataType.equalsIgnoreCase(ProjectConstant.TYPE_IPAD_APPS) || dataType.equalsIgnoreCase(ProjectConstant.TYPE_MOBILE_APPS)) {
            String ecommUserID = this.getEComUserID();
            if (ecommUserID != null && ecommUserID.length() > 0) {
                return true;
            } else {
                return false;
            }
        } else {
            int position = 0;
            try {
                // post_evar68
                position = Integer.parseInt(conf.get("POST_EVAR69"));
                String authStatus = rawFields[position].trim();
                if (authStatus.length() > 0) {
                    if (authStatus.equalsIgnoreCase("identified") || authStatus.equalsIgnoreCase("authenticated")) {
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            } catch (Exception aie) {
                return false;
            }
        }
    }
}
