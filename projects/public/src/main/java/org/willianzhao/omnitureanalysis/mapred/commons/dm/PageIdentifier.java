package org.willianzhao.omnitureanalysis.mapred.commons.dm;

import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by willianzhao on 3/25/14.
 */
public class PageIdentifier extends SmartIdentifier {

    String pageType;
    String pageName;
    String pageEvent;
    String pageURL;
    String pageReferrer;
    // this field is coming from evar28 or post_evar28
    String transactionID;
	private static Logger logger = LoggerFactory.getLogger(PageIdentifier.class);

    public PageIdentifier(Configuration config, String[] inputFields) {
        super(config, inputFields);
    }

    @Deprecated
    public boolean isSpecifiedTypeOfPage(String searchKeyWord) {
        if (pageType == null) {
            pageType = this.getPageType();
        }
        if (pageName == null) {
            pageName = this.getPageName();
        }
        if (pageType.trim().length() > 0) {
            if (pageType.contains(searchKeyWord)) {
                return true;
            }
        }
        if (pageName.trim().length() > 0) {
            if (pageName.contains(searchKeyWord)) {
                return true;
            }
        }
        return false;
    }

    @Deprecated
    public boolean isValidBrowseRecord() {
        boolean validBrowse = false;
        String product_list;
        int position = 0;
        try {
            position = Integer.parseInt(conf.get("PRODUCT_LIST"));
            product_list = rawFields[position].trim();
            if (product_list.length() > 0) {
                validBrowse = true;
                return validBrowse;
            }
        } catch (Exception aie) {
            // do nothing
        }
        String genreID;
        try {
            position = Integer.parseInt(conf.get("POST_GENRE_ID"));
            genreID = rawFields[position].trim();
            if (genreID.length() > 0) {
                validBrowse = true;
            }
        } catch (Exception aie) {
            //do nothing
        }
        return validBrowse;
    }

    /*
    The checkout record is identified by strictly equal to pagename 'Checkout - Thank you'.
    Mobile checkout page will be 'mCheckout - Thank you'.
     */

    public boolean isPCCheckoutRecord() {
        if (pageName == null) {
            pageName = this.getPageName().trim();
        }
        if (pageName.length() > 0 && pageName.equalsIgnoreCase(ProjectConstant.PAGENAME_PC_CHECKOUTPAGE)) {
            //it's equal to 'Checkout - Thank you' for PC
            return true;
        }
        return false;
    }

    public boolean isTransactionIDAvailable() {
        if (transactionID == null) {
            getTransactionID();
        }
        return transactionID != null && transactionID.length() > 0;
    }

    public String getTransactionID() {
        if (transactionID != null && transactionID.length() > ProjectConstant.CONSTANT_MIN_ID_LENGTH) {
            return transactionID;
        } else {
            int position = 0;
            try {
                // try the pre field
                position = Integer.parseInt(conf.get("EVAR28"));
                String transID = rawFields[position].trim();
                if (transID.length() > ProjectConstant.CONSTANT_MIN_ID_LENGTH) {
                    try {
                        // check if the transaction ID has illegal chars
                        int i = Integer.parseInt(transID);
                        if (i > 0) {
                            transactionID = transID;
                        } else {
                            transactionID = "";
                        }
                    } catch (NumberFormatException cce) {
                        transactionID = "";
                    }
                }
                if (transactionID.trim().length() == 0) {
                    // try the post-field
                    position = Integer.parseInt(conf.get("POST_EVAR28"));
                    transID = rawFields[position].trim();
                    if (transID.length() > ProjectConstant.CONSTANT_MIN_ID_LENGTH) {
                        try {
                            // check if the transaction ID has illegal chars
                            int i = Integer.parseInt(transID);
                            if (i > 0) {
                                transactionID = transID;
                            } else {
                                transactionID = "";
                            }
                        } catch (NumberFormatException cce) {
                            transactionID = "";
                        }
                    }
                }
                return transactionID;

            } catch (Exception e) {
                return "";
            }
        }
    }

    public String getPageType() {
        if (pageType != null) {
            return pageType;
        } else {
            int position = 0;
            try {
                position = Integer.parseInt(conf.get("PROP11"));
                String pageType = rawFields[position].trim();
                if (pageType.length() > 0) {
                    return pageType;
                } else {
                    return "";
                }
            } catch (Exception aie) {
                return "";
            }
        }
    }

    public String getPageName() {
        if (pageName != null) {
            return pageName;
        } else {
            int position = 0;
            try {
                position = Integer.parseInt(conf.get("PAGENAME"));
                String pageName = rawFields[position].trim();
                if (pageName.length() > 0) {
                    return pageName;
                } else {
                    return "";
                }
            } catch (Exception aie) {
                return "";
            }
        }
    }

    public String getPageURL() {
        if (pageURL == null) {
            int position = 0;
            try {
                position = Integer.parseInt(conf.get("PAGE_URL"));
                String localPageURL = rawFields[position].trim();
                if (localPageURL.length() > 0) {
                    pageURL = localPageURL;
                } else {
                    pageURL = "";
                }
            } catch (Exception aie) {
                pageURL = "";
            }
        }
        return pageURL;
    }

    public String getPageEvent() {
        if (pageEvent == null) {
            int position = 0;
            try {
                position = Integer.parseInt(conf.get("PAGE_EVENT"));
                String localPageEvent = rawFields[position].trim();
                if (localPageEvent.length() > 0) {
                    pageEvent = localPageEvent;
                } else {
                    pageEvent = "";
                }
            } catch (Exception aie) {
                pageEvent = "";
            }
        }
        return pageEvent;
    }

    public String getPageReferrer() {
        if (pageReferrer == null) {
            int position = 0;
            try {
                position = Integer.parseInt(conf.get("REFERRER"));
                String localPageReferrer = rawFields[position].trim();
                if (localPageReferrer.length() > 0) {
                    pageReferrer = localPageReferrer;
                } else {
                    pageReferrer = "";
                }
            } catch (Exception aie) {
                pageReferrer = "";
            }
        }
        return pageReferrer;
    }

}
