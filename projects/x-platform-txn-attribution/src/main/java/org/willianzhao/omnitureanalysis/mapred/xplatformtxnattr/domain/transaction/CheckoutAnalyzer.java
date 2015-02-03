package org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transaction;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.willianzhao.omnitureanalysis.mapred.commons.misc.BrowseHistoryComparator;
import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;
import org.willianzhao.omnitureanalysis.mapred.commons.model.BrowseBehaviorMetrics;
import org.willianzhao.omnitureanalysis.mapred.commons.model.BrowseHistory;
import org.willianzhao.omnitureanalysis.mapred.commons.model.CheckoutPageKey;
import org.willianzhao.omnitureanalysis.mapred.commons.model.GenreNode;
import org.willianzhao.omnitureanalysis.mapred.commons.model.GenreTreeAbstract;
import org.willianzhao.omnitureanalysis.mapred.commons.model.VisitStep;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.mapfile.UserMapFileFactorySmokeTest;

/**
 * Created by willianzhao on 5/3/14.
 */
public class CheckoutAnalyzer {


    Configuration conf;
    MapFile.Reader eventMapFileReader;
    Reducer.Context context;
    SimpleDateFormat simpleFormatter;
    ArrayList<CheckoutPageKey> checkoutRecs = new ArrayList<CheckoutPageKey>();
    ArrayList<BrowseHistory> browseRecs = new ArrayList<BrowseHistory>();
    HashMap<String, HashMap<Integer, GenreNode>> genreLeafMap;

	private static Logger logger = LoggerFactory.getLogger(UserMapFileFactorySmokeTest.class);

    public CheckoutAnalyzer(Reducer.Context context, Iterable<VisitStep> values,
                            HashMap<String, HashMap<Integer, GenreNode>> genreLeafMap,
                            MapFile.Reader eventMapFileReader, SimpleDateFormat simpleFormatter) {
        this.eventMapFileReader = eventMapFileReader;
        this.conf = context.getConfiguration();
        this.context = context;
        this.genreLeafMap = genreLeafMap;
        this.simpleFormatter = simpleFormatter;
        ArrayList<VisitStep> allRecs = new ArrayList<VisitStep>();
        for (VisitStep value : values) {
            try {
                VisitStep copy = ReflectionUtils.copy(conf, value, new VisitStep());
                allRecs.add(copy);

            } catch (Exception e) {
                //e.printStackTrace(logger.getStream(Level.ERROR));
            }

        }
        classifyRecs(allRecs);
        correctTicketIDforCheckout();
        correctEventIDforCheckout();
        fillGenreInfoforCheckout();

        /*
        Sort the browse records
         */
        BrowseHistoryComparator comparator = new BrowseHistoryComparator();
        Collections.sort(browseRecs, comparator);

    }

    void classifyRecs(ArrayList<VisitStep> allRecs) {
        for (VisitStep step : allRecs) {
            String actionType = step.getActionType();
            String dataSource = step.getDataSource();
            if (dataSource.equals(ProjectConstant.SOURCE_US_WEB) || dataSource.equals(
                    ProjectConstant.SOURCE_UK_WEB) || dataSource.equals(ProjectConstant.SOURCE_STUB_TRANS)) {
                if (actionType.equals(ProjectConstant.ACTION_CHECKOUT)) {
                    CheckoutPageKey pageKey = null;
                    try {
                        pageKey = convertVisitStepToCheckoutKey(step);
                        checkoutRecs.add(pageKey);

                    } catch (Exception e) {
                        logger.error("Failed to convert the step to visit step object");
                    }

                }
            } else if (!dataSource.equals(ProjectConstant.SOURCE_OTHERS)) {
                // the source is for mobile : mweb, ipad, mobileApp
                if (actionType.equals(ProjectConstant.ACTION_BROWSE)) {
                    try {
                        BrowseHistory bh = convertVisitStepToBrowseHistory(step);
                        browseRecs.add(bh);
                        logger.trace("Success add the browse record '{}'", bh.toString());
                    } catch (Exception e) {
                        logger.error("Failed to convert the step to browse history object");
                        //e.printStackTrace(logger.getStream(Level.ERROR));
                    }

                }

            }
        }
    }

    CheckoutPageKey convertVisitStepToCheckoutKey(VisitStep step) throws Exception {
        String userID = step.getUserID();
        String buyerRegisterTime = step.getBuyerRegisterTime();
        String visitTime = step.getVisitTime();
        String ticketID = step.getTicketID();
        String transactionID = step.getTransactionID();
        String eventID = step.getEventID();
        String genreID = step.getGenreID();
        CheckoutPageKey pageKey = new CheckoutPageKey(userID, buyerRegisterTime, visitTime, eventID, genreID, ticketID,
                transactionID);
        String eventDesc = this.getEventDescFromMapFile(eventID);
        pageKey.setPurchaseEventDesc(eventDesc);
        return pageKey;
    }

    BrowseHistory convertVisitStepToBrowseHistory(VisitStep step) throws Exception {
        String userID = step.getUserID();
        String visitTime = step.getVisitTime();
        String ticketID = step.getTicketID();
        String eventID = step.getEventID();
        String genreID = step.getGenreID();
        String dataSource = step.getDataSource();
        BrowseHistory bh = new BrowseHistory(dataSource, userID, visitTime, ticketID, eventID, genreID);
        return bh;

    }

    private boolean fillGenreHierarchy(String firstLevelGenreID, GenreTreeAbstract genreTree) {
        if (firstLevelGenreID != null && firstLevelGenreID.length() > 0) {
            if (genreLeafMap != null) {
                String gcfID;
                HashMap<Integer, GenreNode> genreLevelMap = genreLeafMap.get(firstLevelGenreID);
                if (genreLevelMap != null) {
                    String genreDescTemp;
                    String genreIDTemp;
                    GenreNode gn = genreLevelMap.get(1);
                    if (gn != null) {
                        genreDescTemp = gn.getGenreDescription();
                        genreTree.setLevel1GenreDesc(genreDescTemp);
                        /*
                        First try to get genre cut final for the genre level 1.
                        Because all the genre in the same hierarchy have the same gcf.
                         */
                        gcfID = gn.getGcfID();
                        if (gcfID != null && gcfID.trim().length() != 0) {
                            String gcf = conf.get(ProjectConstant.GENRE_CUT_FINAL_CONF_PREFIX + gcfID);
                            if (gcf != null) {
                                genreTree.setGcf(gcf);
                            } else {
                                gcfID = ProjectConstant.CONSTANT_UNFOUND_FLAG;
                            }
                        } else {
                            gcfID = ProjectConstant.CONSTANT_UNFOUND_FLAG;
                        }
                    } else {
                        genreTree.setLevel1GenreDesc("");
                        //for debug
                      //RM context.getCounter("Debug", "Failed to get the level 1 genre description").increment(1);
                        return false;
                    }
                    gn = genreLevelMap.get(2);
                    if (gn != null) {
                        genreDescTemp = gn.getGenreDescription();
                        genreIDTemp = gn.getGenreID();
                        genreTree.setLevel2GenreID(genreIDTemp);
                        genreTree.setLevel2GenreDesc(genreDescTemp);
                        if (gcfID.equals(ProjectConstant.CONSTANT_UNFOUND_FLAG)) {
                        /*
                        If genre level 1 doesn't have the gcfID then try to get genre cut final for the genre level 2.
                        Because all the genre in the same hierarchy have the same gcf.
                         */
                            gcfID = gn.getGcfID();
                            if (gcfID != null && gcfID.trim().length() != 0) {
                                String gcf = conf.get(ProjectConstant.GENRE_CUT_FINAL_CONF_PREFIX + gcfID);
                                if (gcf != null) {
                                    genreTree.setGcf(gcf);
                                } else {
                                    gcfID = ProjectConstant.CONSTANT_UNFOUND_FLAG;
                                }
                            } else {
                                gcfID = ProjectConstant.CONSTANT_UNFOUND_FLAG;
                            }
                        }
                    } else {
                        //From the second level of genre, if no genre level existed, return true;
                        return true;
                    }
                    gn = genreLevelMap.get(3);
                    if (gn != null) {
                        genreDescTemp = gn.getGenreDescription();
                        genreIDTemp = gn.getGenreID();
                        genreTree.setLevel3GenreID(genreIDTemp);
                        genreTree.setLevel3GenreDesc(genreDescTemp);
                        if (gcfID.equals(ProjectConstant.CONSTANT_UNFOUND_FLAG)) {
                        /*
                        If genre level 2 doesn't have the gcfID then try to get genre cut final for the genre level 3.
                        Because all the genre in the same hierarchy have the same gcf.
                         */
                            gcfID = gn.getGcfID();
                            if (gcfID != null && gcfID.trim().length() != 0) {
                                String gcf = conf.get(ProjectConstant.GENRE_CUT_FINAL_CONF_PREFIX + gcfID);
                                if (gcf != null) {
                                    genreTree.setGcf(gcf);
                                } else {
                                    gcfID = ProjectConstant.CONSTANT_UNFOUND_FLAG;
                                }
                            } else {
                                gcfID = ProjectConstant.CONSTANT_UNFOUND_FLAG;
                            }
                        }
                    } else {
                        return true;
                    }
                    gn = genreLevelMap.get(4);
                    if (gn != null) {
                        genreDescTemp = gn.getGenreDescription();
                        genreIDTemp = gn.getGenreID();
                        genreTree.setLevel4GenreID(genreIDTemp);
                        genreTree.setLevel4GenreDesc(genreDescTemp);
                        if (gcfID.equals(ProjectConstant.CONSTANT_UNFOUND_FLAG)) {
                        /*
                        If genre level 3 doesn't have the gcfID then try to get genre cut final for the genre level 4.
                        Because all the genre in the same hierarchy have the same gcf.
                         */
                            gcfID = gn.getGcfID();
                            if (gcfID != null && gcfID.trim().length() != 0) {
                                String gcf = conf.get(ProjectConstant.GENRE_CUT_FINAL_CONF_PREFIX + gcfID);
                                if (gcf != null) {
                                    genreTree.setGcf(gcf);
                                } else {
                                    gcfID = ProjectConstant.CONSTANT_UNFOUND_FLAG;
                                }
                            } else {
                                gcfID = ProjectConstant.CONSTANT_UNFOUND_FLAG;
                            }
                        }
                    } else {
                        return true;
                    }
                    gn = genreLevelMap.get(5);
                    if (gn != null) {
                        genreDescTemp = gn.getGenreDescription();
                        genreIDTemp = gn.getGenreID();
                        genreTree.setLevel5GenreID(genreIDTemp);
                        genreTree.setLevel5GenreDesc(genreDescTemp);
                        if (gcfID.equals(ProjectConstant.CONSTANT_UNFOUND_FLAG)) {
                        /*
                        If genre level 4 doesn't have the gcfID then try to get genre cut final for the genre level 5.
                        Because all the genre in the same hierarchy have the same gcf.
                         */
                            gcfID = gn.getGcfID();
                            if (gcfID != null && gcfID.trim().length() != 0) {
                                String gcf = conf.get(ProjectConstant.GENRE_CUT_FINAL_CONF_PREFIX + gcfID);
                                if (gcf != null) {
                                    genreTree.setGcf(gcf);
                                }
                            }
                        }
                    } else {
                        return true;
                    }
                    gn = genreLevelMap.get(6);
                    if (gn != null) {
                        genreDescTemp = gn.getGenreDescription();
                        genreIDTemp = gn.getGenreID();
                        genreTree.setLevel6GenreID(genreIDTemp);
                        genreTree.setLevel6GenreDesc(genreDescTemp);

                    } else {
                        return true;
                    }
                    return true;

                }

            } else {
                //for debug
            	//RM context.getCounter("Debug", "genreLeafMap is null").increment(1);
            }

        }
        return false;
    }

    public ArrayList<BrowseHistory> getFootprintsBeforeCheckout(String checkoutTime) {
        //The list will contain only once for the same event or genre.Because the list is sorted descendant, then the latest record for same event or genre will be put into list.
        //Set the limit to the earliest browse record
        int limit;
        String duration = conf.get(ProjectConstant.PARAM_LABEL_DURATION);
        if (duration == null) {
            limit = Integer.parseInt(ProjectConstant.LOOKAHEAD_DAYS) + 1;
        } else {
            limit = Integer.parseInt(duration) + 1;
        }
        ArrayList<BrowseHistory> bha = new ArrayList<BrowseHistory>();
        HashMap<String, String> eventGenreMap = new HashMap<String, String>();
        try {
            //Because checkoutDate is UTC timezone, so we need to convert it to PDT to work with Mobile data
            Calendar checkoutCalendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            Date transDateGMT = simpleFormatter.parse(checkoutTime);
            checkoutCalendar.setTime(transDateGMT);
            checkoutCalendar.add(Calendar.HOUR_OF_DAY, -7);
            Date checkoutDateTime = checkoutCalendar.getTime();
            Calendar limitBrowseCalendar = Calendar.getInstance();
            limitBrowseCalendar.setTime(checkoutDateTime);
            limitBrowseCalendar.add(Calendar.DATE, -limit);
            //The records have been sorted by the business key
            for (BrowseHistory bh : browseRecs) {
                String visitTime = bh.getVisitTime();
                Date visitDateTime = simpleFormatter.parse(visitTime);
                Calendar browseCalendar = Calendar.getInstance();
                browseCalendar.setTime(visitDateTime);
                if (browseCalendar.before(checkoutCalendar)) {
                	//RM context.getCounter("Debug", "Mobile records before the pc checkout time").increment(1);
                    if (!browseCalendar.before(limitBrowseCalendar)) {
                    	//RM context.getCounter("Debug",
                    	//RM         "Before the pc checkout time and later than the limit " + limit + " day(s)").increment(
                    	//RM        1);
                        String eventID = bh.getEventID();
                        String genreID = bh.getLevel1GenreID();
                        if (eventMapFileReader != null && genreID.trim().length() == 0) {
                            if (eventID != null && eventID.trim().length() > 0) {

                        /*
                        Reuse the existing genreID and reduce the mapfile lookup times
                         */
                                try {
                                    int i = Integer.parseInt(eventID);
                                    //no ClassCastException
                                    String cachedGenreID = eventGenreMap.get(eventID);
                                    if (cachedGenreID == null || cachedGenreID.trim().length() == 0) {
                                        genreID = this.getGenreIDFromMapFile(eventID);
                                      //RM context.getCounter("Debug", "Lookup events Mapfile times").increment(1);
                                        eventGenreMap.put(eventID, genreID);

                                    } else {
                                        genreID = cachedGenreID;
                                    }
                                    bh.setLevel1GenreID(genreID);
                                    if (genreID.trim().length() > 0) {
                                        try {
                                            fillGenreHierarchy(genreID, bh);
                                        } catch (Exception e) {
                                            // do nothing
                                        }
                                    }
                                } catch (NumberFormatException cce) {
                                    //do nothing
                                }
                            } else {
                                //if the eventID and genreID are both null then move next
                                continue;
                            }
                        }
                        bha.add(bh);

                    } else {
                        // the browsing records are earlier than the limit of duration
                    	//RM context.getCounter("Debug", "Mobile records earlier than the limit").increment(1);

                    }

                } else {
                    // the browsing records are earlier than the limit of duration
                	//RM context.getCounter("Debug", "Mobile records later than the pc checkout time").increment(1);

                }
            }
            if (bha.size() > 1) {
                //Sort the new browse history list.
                BrowseHistoryComparator comparator = new BrowseHistoryComparator();
                Collections.sort(bha, comparator);

            }
        } catch (Exception e) {
            // do nothing
        }
        return bha;
    }

    public ArrayList<CheckoutPageKey> getCheckoutRecs() {
        return checkoutRecs;
    }

    public BrowseBehaviorMetrics analyzeBrowseBehaviorForCheckout(CheckoutPageKey checkoutRec,
                                                                  ArrayList<BrowseHistory> browseList) {
        if (checkoutRec != null) {
            String userID = checkoutRec.getUserID();
            if (userID != null && userID.length() > 0) {
                BrowseBehaviorMetrics bm = new BrowseBehaviorMetrics(userID);
                String buyerRegisterTime = checkoutRec.getBuyerRegisterTime();
                String purchaseTime = checkoutRec.getPurchaseTime();
                String ticketID = checkoutRec.getPurchaseTicketID();
                String transactionID = checkoutRec.getTransactionID();
                String purchaseEventID = checkoutRec.getPurchaseEventID();
                String purchaseEventDesc = checkoutRec.getPurchaseEventDesc();
                String purchaseGenreID = checkoutRec.getPurchaseGenreID();
                String purchaseGenreDesc = checkoutRec.getPurchaseGenreDesc();
                String genreCutFinal = checkoutRec.getGcf();
                try {
                    bm.setIsNewUser(isNewUser(buyerRegisterTime));
                } catch (Exception e) {
                    bm.setIsNewUser(0);
                }
                bm.setPurchaseTime(purchaseTime);
                bm.setTicketID(ticketID);
                bm.setTransactionID(transactionID);
                bm.setPurchaseEventID(purchaseEventID);
                bm.setPurchaseEventDesc(purchaseEventDesc);
                bm.setPurchaseGenreID(purchaseGenreID);
                bm.setPurchaseGenreDescLevel1(purchaseGenreDesc);
                bm.setGcf(genreCutFinal);
                analyzeUsmwebPlatform(checkoutRec, browseList, bm);
                analyzeIPadPlatform(checkoutRec, browseList, bm);
                analyzeMobilePlatform(checkoutRec, browseList, bm);
                bm.setTop2ndGenre(checkoutRec.getTop2ndGenreDesc());
                /*
                If browsing list is empty then give defaults values
                 */
                int browseListSize = browseList.size();
                if (browseListSize == 0) {
                    bm.setFlagOthers("1");

                } else {
                    if (bm.getUsmwebFlag2a().equals("1") || bm.getIpadFlag2a().equals(
                            "1") || bm.getMobileFlag2a().equals("1")) {
                        bm.setFlag2a("1");
                    } else if (bm.getUsmwebFlag2b().equals("1") || bm.getIpadFlag2b().equals(
                            "1") || bm.getMobileFlag2b().equals("1")) {
                        bm.setFlag2b("1");
                    } else if (bm.getUsmwebFlag2a().equals("0") && bm.getUsmwebFlag2b().equals("0")
                            && bm.getIpadFlag2a().equals("0") && bm.getIpadFlag2b().equals(
                            "0") && bm.getMobileFlag2a().equals("0") && bm.getMobileFlag2b().equals("0")) {
                        bm.setFlag2c("1");

                    }
                    bm.setFlagOthers("0");

                }
                return bm;
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    int isNewUser(String buyerRegisterTime) throws Exception {
        String startDateStr = conf.get(ProjectConstant.PARAM_LABEL_STARTDATE);
        String endDateStr = conf.get(ProjectConstant.PARAM_LABEL_ENDDATE);
        String duration = conf.get(ProjectConstant.PARAM_LABEL_DURATION);
        if (duration == null) {
            duration = ProjectConstant.LOOKAHEAD_DAYS;
        }
        Calendar cal = Calendar.getInstance();
        Calendar cal1 = Calendar.getInstance();
        SimpleDateFormat simpleFormatter = new SimpleDateFormat(
                ProjectConstant.CONSTANT_PARAM_DATE_PATTERN);
        SimpleDateFormat buyerRegisterDateFormat = new SimpleDateFormat(ProjectConstant.CONSTANT_PARAM_DATE_PATTERN);
        Date initialProcessStartDate = simpleFormatter.parse(startDateStr);
        Date processStartDate = simpleFormatter.parse(startDateStr);
        Date processEndDate = simpleFormatter.parse(endDateStr);
        String[] temp = buyerRegisterTime.split(" ");
        Date buyerRegistrationDate = null;
        if (temp.length > 1) {
            buyerRegistrationDate = buyerRegisterDateFormat.parse(temp[0]);
        } else {
            buyerRegistrationDate = buyerRegisterDateFormat.parse(buyerRegisterTime);
        }
        cal.setTime(processStartDate);
        cal.add(Calendar.DATE, -new Integer(1));
        initialProcessStartDate = cal.getTime();
        cal1.setTime(processEndDate);
        cal1.add(Calendar.DATE, 1);
        processEndDate = cal1.getTime();
        if (buyerRegistrationDate.after(processStartDate)
                && buyerRegistrationDate.before(processEndDate)) {
            return 1;
        } else {
            return 0;
        }
    }

    void analyzeUsmwebPlatform(CheckoutPageKey checkoutRec, ArrayList<BrowseHistory> browseList,
                               BrowseBehaviorMetrics bm) {
        String purchaseEventID = checkoutRec.getPurchaseEventID();
        String purchaseGenreID = checkoutRec.getPurchaseGenreID();
        String checkoutGenreLevel2ID = checkoutRec.getLevel2GenreID();
        String checkoutGenreLevel3ID = checkoutRec.getLevel3GenreID();
        String checkoutGenreLevel4ID = checkoutRec.getLevel4GenreID();
        String checkoutGenreLevel5ID = checkoutRec.getLevel5GenreID();
        int eventHit = 0;
        int genreLevel1Hit = 0;
        int genreLevel2Hit = 0;
        int genreLevel3Hit = 0;
        int genreLevel4Hit = 0;
        int genreLevel5Hit = 0;
        if (purchaseEventID != null && purchaseEventID.length() > 0 && checkEventBrowsed(purchaseEventID, browseList,
                ProjectConstant.SOURCE_US_MWEB)) {
            eventHit++;

        }
        if (checkGenreBrowsed(purchaseGenreID, browseList, ProjectConstant.SOURCE_US_MWEB)) {
            genreLevel1Hit++;
        }
        if (checkGenreBrowsed(checkoutGenreLevel2ID, browseList, ProjectConstant.SOURCE_US_MWEB)) {
            genreLevel2Hit++;
        }
        if (checkGenreBrowsed(checkoutGenreLevel3ID, browseList, ProjectConstant.SOURCE_US_MWEB)) {
            genreLevel3Hit++;
        }
        if (checkGenreBrowsed(checkoutGenreLevel4ID, browseList, ProjectConstant.SOURCE_US_MWEB)) {
            genreLevel4Hit++;
        }
        if (checkGenreBrowsed(checkoutGenreLevel5ID, browseList, ProjectConstant.SOURCE_US_MWEB)) {
            genreLevel5Hit++;
        }
        //Finish the analysis. Fill the result object
        bm.setUsmwebmatchEventCnt(eventHit);
        bm.setUsmwebmatchLevel1GenreCnt(genreLevel1Hit);
        bm.setUsmwebmatchLevel2GenreCnt(genreLevel2Hit);
        bm.setUsmwebmatchLevel3GenreCnt(genreLevel3Hit);
        bm.setUsmwebmatchLevel4GenreCnt(genreLevel4Hit);
        bm.setUsmwebmatchLevel5GenreCnt(genreLevel5Hit);
        //Exclusively set the flag
        if (eventHit > 0) {
            bm.setUsmwebFlag2a("1");

        } else if (genreLevel1Hit + genreLevel2Hit + genreLevel3Hit + genreLevel4Hit + genreLevel5Hit > 0) {
            bm.setUsmwebFlag2b("1");

        }

    }

    void analyzeMobilePlatform(CheckoutPageKey checkoutRec, ArrayList<BrowseHistory> browseList,
                               BrowseBehaviorMetrics bm) {
        String purchaseEventID = checkoutRec.getPurchaseEventID();
        String purchaseGenreID = checkoutRec.getPurchaseGenreID();
        String checkoutGenreLevel2ID = checkoutRec.getLevel2GenreID();
        String checkoutGenreLevel3ID = checkoutRec.getLevel3GenreID();
        String checkoutGenreLevel4ID = checkoutRec.getLevel4GenreID();
        String checkoutGenreLevel5ID = checkoutRec.getLevel5GenreID();
        int eventHit = 0;
        int genreLevel1Hit = 0;
        int genreLevel2Hit = 0;
        int genreLevel3Hit = 0;
        int genreLevel4Hit = 0;
        int genreLevel5Hit = 0;
        if (purchaseEventID != null && purchaseEventID.length() > 0 && checkEventBrowsed(purchaseEventID, browseList,
                ProjectConstant.SOURCE_MOBILE)) {
            eventHit++;
        }
        if (checkGenreBrowsed(purchaseGenreID, browseList, ProjectConstant.SOURCE_MOBILE)) {
            genreLevel1Hit++;

        }
        if (checkGenreBrowsed(checkoutGenreLevel2ID, browseList, ProjectConstant.SOURCE_MOBILE)) {
            genreLevel2Hit++;

        }
        if (checkGenreBrowsed(checkoutGenreLevel3ID, browseList, ProjectConstant.SOURCE_MOBILE)) {
            genreLevel3Hit++;
        }
        if (checkGenreBrowsed(checkoutGenreLevel4ID, browseList, ProjectConstant.SOURCE_MOBILE)) {
            genreLevel4Hit++;
        }
        if (checkGenreBrowsed(checkoutGenreLevel5ID, browseList, ProjectConstant.SOURCE_MOBILE)) {
            genreLevel5Hit++;
        }
        //Finish the analysis. Fill the result object
        bm.setMobilematchEventCnt(eventHit);
        bm.setMobilematchLevel1GenreCnt(genreLevel1Hit);
        bm.setMobilematchLevel2GenreCnt(genreLevel2Hit);
        bm.setMobilematchLevel3GenreCnt(genreLevel3Hit);
        bm.setMobilematchLevel4GenreCnt(genreLevel4Hit);
        bm.setMobilematchLevel5GenreCnt(genreLevel5Hit);
        //Exclusively set the flag
        if (eventHit > 0) {
            bm.setMobileFlag2a("1");

        } else if (genreLevel1Hit + genreLevel2Hit + genreLevel3Hit + genreLevel4Hit + genreLevel5Hit > 0) {
            bm.setMobileFlag2b("1");

        }

    }

    void analyzeIPadPlatform(CheckoutPageKey checkoutRec, ArrayList<BrowseHistory> browseList,
                             BrowseBehaviorMetrics bm) {
        String purchaseEventID = checkoutRec.getPurchaseEventID();
        String purchaseGenreID = checkoutRec.getPurchaseGenreID();
        String checkoutGenreLevel2ID = checkoutRec.getLevel2GenreID();
        String checkoutGenreLevel3ID = checkoutRec.getLevel3GenreID();
        String checkoutGenreLevel4ID = checkoutRec.getLevel4GenreID();
        String checkoutGenreLevel5ID = checkoutRec.getLevel5GenreID();
        int eventHit = 0;
        int genreLevel1Hit = 0;
        int genreLevel2Hit = 0;
        int genreLevel3Hit = 0;
        int genreLevel4Hit = 0;
        int genreLevel5Hit = 0;
        if (purchaseEventID != null && purchaseEventID.length() > 0 && (checkEventBrowsed(purchaseEventID, browseList,
                ProjectConstant.SOURCE_IPAD) || checkEventBrowsed(purchaseEventID, browseList,
                        ProjectConstant.SOURCE_IOS))) {
            eventHit++;
        }
        if (checkGenreBrowsed(purchaseGenreID, browseList, ProjectConstant.SOURCE_IPAD) || checkEventBrowsed(purchaseEventID, browseList,
                ProjectConstant.SOURCE_IOS)) {
            genreLevel1Hit++;

        }
        if (checkGenreBrowsed(checkoutGenreLevel2ID, browseList, ProjectConstant.SOURCE_IPAD) || checkEventBrowsed(checkoutGenreLevel2ID, browseList,
                ProjectConstant.SOURCE_IOS)) {
            genreLevel2Hit++;

        }
        if (checkGenreBrowsed(checkoutGenreLevel3ID, browseList, ProjectConstant.SOURCE_IPAD) || checkEventBrowsed(checkoutGenreLevel3ID, browseList,
                ProjectConstant.SOURCE_IOS)) {
            genreLevel3Hit++;
        }
        if (checkGenreBrowsed(checkoutGenreLevel4ID, browseList, ProjectConstant.SOURCE_IPAD) || checkEventBrowsed(checkoutGenreLevel4ID, browseList,
                ProjectConstant.SOURCE_IOS)) {
            genreLevel4Hit++;
        }
        if (checkGenreBrowsed(checkoutGenreLevel5ID, browseList, ProjectConstant.SOURCE_IPAD) || checkEventBrowsed(checkoutGenreLevel5ID, browseList,
                ProjectConstant.SOURCE_IOS)) {
            genreLevel5Hit++;
        }
        //Finish the analysis. Fill the result object
        bm.setIpadmatchEventCnt(eventHit);
        bm.setIpadmatchLevel1GenreCnt(genreLevel1Hit);
        bm.setIpadmatchLevel2GenreCnt(genreLevel2Hit);
        bm.setIpadmatchLevel3GenreCnt(genreLevel3Hit);
        bm.setIpadmatchLevel4GenreCnt(genreLevel4Hit);
        bm.setIpadmatchLevel5GenreCnt(genreLevel5Hit);
        //Exclusively set the flag
        if (eventHit > 0) {
            bm.setIpadFlag2a("1");

        } else if (genreLevel1Hit + genreLevel2Hit + genreLevel3Hit + genreLevel4Hit + genreLevel5Hit > 0) {
            bm.setIpadFlag2b("1");
        }

    }

    boolean checkEventBrowsed(String eventID, ArrayList<BrowseHistory> browseList, String dataSource) {
        boolean foundEventBrowsed = false;
        for (BrowseHistory bh : browseList) {
            String ds = bh.getDataSource();
            if (ds.equals(dataSource)) {
                String browseEventID = bh.getEventID();
                if (eventID.equals(browseEventID)) {
                    foundEventBrowsed = true;
                    break;
                }
            }
        }
        return foundEventBrowsed;
    }

    boolean checkGenreBrowsed(String genreID, ArrayList<BrowseHistory> browseList, String dataSource) {
        if (genreID == null || genreID.trim().length() == 0) {
            return false;
        } else {
            boolean foundGenreBrowsed = false;
            for (BrowseHistory bh : browseList) {
                String ds = bh.getDataSource();
                if (ds.equals(dataSource)) {
                    String browseGenreLevel1ID = bh.getLevel1GenreID();
                    String browseGenreLevel2ID = bh.getLevel2GenreID();
                    String browseGenreLevel3ID = bh.getLevel3GenreID();
                    String browseGenreLevel4ID = bh.getLevel4GenreID();
                    String browseGenreLevel5ID = bh.getLevel5GenreID();
                    // Check the genreID within 3 levels of genre in browsing record
                    if (genreID.equals(browseGenreLevel1ID)) {
                        foundGenreBrowsed = true;
                        break;
                    }
                    if (genreID.equals(browseGenreLevel2ID)) {
                        foundGenreBrowsed = true;
                        break;
                    }
                    if (genreID.equals(browseGenreLevel3ID)) {
                        foundGenreBrowsed = true;
                    }
                    if (browseGenreLevel4ID != null && genreID.equals(browseGenreLevel4ID)) {
                        foundGenreBrowsed = true;
                    }
                    if (browseGenreLevel5ID != null && genreID.equals(browseGenreLevel5ID)) {
                        foundGenreBrowsed = true;
                    }
                }
            }
            return foundGenreBrowsed;
        }
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

    public String getValueFromEventMapFile(String eventID1) {
        String valueStr = "";
        if (eventID1 != null) {
            try {
                LongWritable eventKey = new LongWritable(Long.parseLong(eventID1));
                Text value = new Text();
                Writable val = eventMapFileReader.get(eventKey, value);
                Text temp = val != null ? (Text) val : null;
                if (temp != null) {
                    valueStr = temp.toString();
                }
            } catch (Exception e) {
                //do nothing
            }
        }
        return valueStr;
    }

    private void correctTicketIDforCheckout() {
        HashMap<String, String> tempMap = new HashMap<String, String>();
        for (CheckoutPageKey checkoutRec : checkoutRecs) {
            String ticketID = checkoutRec.getPurchaseTicketID();
            String transactionID = checkoutRec.getTransactionID();
            if (transactionID != null && transactionID.length() > 0 && !tempMap.containsKey(transactionID)) {
                if (ticketID != null && ticketID.length() > 0) {
                    tempMap.put(transactionID, ticketID);
                }
            }
        }
        if (tempMap.size() > 0) {
            for (CheckoutPageKey checkoutRec : checkoutRecs) {
                String ticketID = checkoutRec.getPurchaseTicketID();
                String transactionID = checkoutRec.getTransactionID();
                if ((ticketID == null || ticketID.length() == 0) && transactionID != null && transactionID.length() > 0) {
                    String storedTicketID = tempMap.get(transactionID);
                    checkoutRec.setPurchaseTicketID(storedTicketID);
                }

            }
        }
    }

    private void correctEventIDforCheckout() {
        HashMap<String, String> tempMap = new HashMap<String, String>();
        for (CheckoutPageKey checkoutRec : checkoutRecs) {
            String ticketID = checkoutRec.getPurchaseTicketID();
            String eventID = checkoutRec.getPurchaseEventID();
            if (ticketID != null && ticketID.length() > 0 && eventID != null && eventID.length() > 0) {
                tempMap.put(ticketID, eventID);
            }
        }
        if (tempMap.size() > 0) {
            for (CheckoutPageKey checkoutRec : checkoutRecs) {
                String ticketID = checkoutRec.getPurchaseTicketID();
                String eventID = checkoutRec.getPurchaseEventID();
                String eventDesc = checkoutRec.getPurchaseEventDesc();
                if (eventID == null || eventID.length() == 0) {
                    String storedEventID = tempMap.get(ticketID);
                    checkoutRec.setPurchaseEventID(storedEventID);
                }
                if (eventDesc == null || eventDesc.length() == 0) {
                    String eventID1 = checkoutRec.getPurchaseEventID();
                    eventDesc = this.getEventDescFromMapFile(eventID1);
                    checkoutRec.setPurchaseEventDesc(eventDesc);
                }
            }
        }

    }

    private void fillGenreInfoforCheckout() {
        for (CheckoutPageKey checkoutRec : checkoutRecs) {
            String eventID = checkoutRec.getPurchaseEventID();
            String genreID = checkoutRec.getPurchaseGenreID();
            if (eventMapFileReader != null && genreID.trim().length() == 0) {
                if (eventID != null && eventID.trim().length() > 0) {
                    genreID = this.getGenreIDFromMapFile(eventID);
                }
                if (genreID.trim().length() > 0) {
                    checkoutRec.setLevel1GenreID(genreID);
                }
            }
            try {
                fillGenreHierarchy(genreID, checkoutRec);
            } catch (Exception e) {
                // do nothing
            }
        }
    }

    public String getGenreLevel1Description(String firstLevelGenreID) {
        String genreDescTemp = "";
        if (firstLevelGenreID != null && firstLevelGenreID.length() > 0) {
            if (genreLeafMap != null) {
                HashMap<Integer, GenreNode> genreLevelMap = genreLeafMap.get(firstLevelGenreID);
                if (genreLevelMap != null) {
                    GenreNode gn = genreLevelMap.get(1);
                    if (gn != null) {
                        genreDescTemp = gn.getGenreDescription();
                    }
                }
            }
        }
        return genreDescTemp;

    }
}
