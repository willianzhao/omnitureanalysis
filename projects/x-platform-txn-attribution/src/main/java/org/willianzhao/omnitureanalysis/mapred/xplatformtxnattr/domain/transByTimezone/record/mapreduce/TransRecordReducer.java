package org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transByTimezone.record.mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;
import org.willianzhao.omnitureanalysis.mapred.commons.model.GenreFactory;
import org.willianzhao.omnitureanalysis.mapred.commons.model.GenreNode;
import org.willianzhao.omnitureanalysis.mapred.commons.model.TransactionRecord;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.mapfile.EventsMapFileFactory;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.mapfile.UserContactsMapFileFactory;

/**
 * Created by weilzhao on 8/21/14.
 */
public class TransRecordReducer extends Reducer<Text, TransactionRecord, NullWritable, TransactionRecord> {

    SimpleDateFormat simpleFormatter;
    Configuration conf = null;
    MapFile.Reader eventMapFileReader;
    MapFile.Reader userContactsMapFileReader;
    HashMap<String, HashMap<Integer, GenreNode>> genreTreeMap;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        super.setup(context);
        this.conf = context.getConfiguration();
        if (eventMapFileReader == null) {
            EventsMapFileFactory eventMapFile;
            try {
                eventMapFile = new EventsMapFileFactory(conf);
                eventMapFileReader = eventMapFile.getMapfileReader();

            } catch (ParseException e) {
                //e.printStackTrace(logger.getStream(Level.ERROR));
            } catch (Exception e) {
                //e.printStackTrace(logger.getStream(Level.ERROR));
            }

        }
        if (userContactsMapFileReader == null) {
            UserContactsMapFileFactory userContactsMapFile;
            try {
                userContactsMapFile = new UserContactsMapFileFactory(conf);
                userContactsMapFileReader = userContactsMapFile.getMapfileReader();

            } catch (ParseException e) {
                //e.printStackTrace(logger.getStream(Level.ERROR));
            } catch (Exception e) {
                //e.printStackTrace(logger.getStream(Level.ERROR));
            }

        }
        if (genreTreeMap == null) {
            String genreNodes = conf.get(ProjectConstant.PATH_GENRENODES_FILE);
            Path genreFile = new Path(genreNodes);
            FileSystem fs = genreFile.getFileSystem(conf);
            if (fs.exists(genreFile)) {
                GenreFactory gf = new GenreFactory(conf, genreFile);
                try {
                    genreTreeMap = gf.getGenreLeafMap();
                } catch (Exception e) {
                    throw new IOException(
                            "Fail to construct genre tree object in memory. Exception message : " + e.getLocalizedMessage());
                }
            } else {
                throw new IOException("The genre file is missing in this node");
            }
        }
        simpleFormatter = new SimpleDateFormat(ProjectConstant.CONSTANT_DATE_PATTERN);
    }

    @Override
    protected void reduce(Text key, Iterable<TransactionRecord> values, Context context) throws IOException, InterruptedException {
        boolean validOrder = false;
        NullWritable outputKey = NullWritable.get();
        String transID = key.toString();
        TransactionRecord tRecord = new TransactionRecord(transID, "");
      //RM context.getCounter("Debug", "Construct transaction ID in reducer").increment(1);
        //Merge the attributes
        Iterator<TransactionRecord> trItr = values.iterator();
        while (trItr.hasNext()) {
        	//RM context.getCounter("Debug", "Enter into the loop body").increment(1);
//            TransactionRecord tR = ReflectionUtils.copy(conf, temp, new TransactionRecord());
            TransactionRecord tR = trItr.next();
            if (tRecord.getTicketID().length() == 0 && tR.getTicketID().length() > 0) {
                tRecord.setTicketID(tR.getTicketID());
              //RM context.getCounter("Debug", "Set ticketID").increment(1);
            }
            if (tRecord.getUserID().length() == 0 && tR.getUserID().length() > 0) {
                tRecord.setUserID(tR.getUserID());
              //RM context.getCounter("Debug", "Set userID").increment(1);
            }
            if (tRecord.getPurchaseTime().length() == 0 && tR.getPurchaseTime().length() > 0) {
                tRecord.setPurchaseTime(tR.getPurchaseTime());
              //RM context.getCounter("Debug", "Set purchaseTime").increment(1);
            }
            if (tRecord.getPurchaseEventID().length() == 0 && tR.getPurchaseEventID().length() > 0) {
                tRecord.setPurchaseEventID(tR.getPurchaseEventID());
              //RM context.getCounter("Debug", "Set eventID").increment(1);
            }
            if (tRecord.getPurchaseGenreID().length() == 0 && tR.getPurchaseGenreID().length() > 0) {
                tRecord.setPurchaseGenreID(tR.getPurchaseGenreID());
              //RM context.getCounter("Debug", "Set genreID").increment(1);
            }
            if (tRecord.getIp().length() == 0 && tR.getIp().length() > 0) {
                tRecord.setIp(tR.getIp());
              //RM context.getCounter("Debug", "Set IP").increment(1);
            }
            if (tRecord.getGeoCity().length() == 0 && tR.getGeoCity().trim().length() > 0) {
                tRecord.setGeoCity(tR.getGeoCity().trim());
              //RM context.getCounter("Debug", "Set GeoCity").increment(1);
            }
            if (tRecord.getGeoRegion().length() == 0 && tR.getGeoRegion().trim().length() > 0) {
                tRecord.setGeoRegion(tR.getGeoRegion().trim());
              //RM context.getCounter("Debug", "Set GeoRegion").increment(1);
            }
            if (tRecord.getGeoCountry().length() == 0 && tR.getGeoCountry().trim().length() > 0) {
                tRecord.setGeoCountry(tR.getGeoCountry().trim());
              //RM context.getCounter("Debug", "Set GeoCountry").increment(1);
            }
            if (tRecord.getGeoZipcode().length() == 0 && tR.getGeoZipcode().trim().length() > 0) {
                tRecord.setGeoZipcode(tR.getGeoZipcode().trim());
              //RM context.getCounter("Debug", "Set Geo Zip code").increment(1);
            }
            //Only process the transaction which has ever come from stub_trans
            if (tR.getDataSource().equals(ProjectConstant.SOURCE_STUB_TRANS)) {
                validOrder = true;
              //RM context.getCounter("Debug", "Output from stub_trans ").increment(1);
            } else {
            	//RM context.getCounter("Debug", "Output from clickstream ").increment(1);
                if (tR.getGeoCity().length() == 0) {
                	//RM context.getCounter("Debug", "Found empty GeoCity ").increment(1);
                }
            }
        }
        if (validOrder) {
            //Get event description
            if (tRecord.getPurchaseEventID().length() > 0) {
                tRecord.setPurchaseEventDesc(this.getEventDescFromMapFile(tRecord.getPurchaseEventID()));
              //RM context.getCounter("Debug", "Set Event Description").increment(1);
            } else {
            	//RM context.getCounter("Debug", "Found empty eventID in transaction").increment(1);
            }
            //Set genre description and GCF
            String firstLevelGenreID = tRecord.getPurchaseGenreID();
            if (firstLevelGenreID.length() > 0) {
                if (genreTreeMap != null) {
                    String gcfID = null;
                    HashMap<Integer, GenreNode> genreLevelMap = genreTreeMap.get(firstLevelGenreID);
                    if (genreLevelMap != null) {
                        String genreDescTemp;
                        GenreNode gn = genreLevelMap.get(1);
                        if (gn != null) {
                            genreDescTemp = gn.getGenreDescription();
                            tRecord.setPurchaseGenreDescLevel1(genreDescTemp);
                        /*
                        First try to get genre cut final for the genre level 1.
                        Because all the genre in the same hierarchy have the same gcf.
                         */
                            gcfID = gn.getGcfID();
                            if (gcfID != null && gcfID.trim().length() != 0) {
                                String gcf = conf.get(ProjectConstant.GENRE_CUT_FINAL_CONF_PREFIX + gcfID);
                                if (gcf != null) {
                                    tRecord.setGcf(gcf);
                                  //RM  context.getCounter("Debug", "Set GCF on the level 1 of genre").increment(1);
                                }
                            } else {
                                gcfID = ProjectConstant.CONSTANT_UNFOUND_FLAG;
                            }
                        }
                        /*If genre level 1 doesn't have the gcfID then try to get genre cut final for the genre level 2.
                        Because all the genre in the same hierarchy have the same gcf.
                        */
                        if (gcfID == null || gcfID.trim().length() == 0 || gcfID.equals(ProjectConstant.CONSTANT_UNFOUND_FLAG)) {
                            gn = genreLevelMap.get(2);
                            if (gn != null) {

                                gcfID = gn.getGcfID();
                                if (gcfID != null && gcfID.trim().length() != 0) {
                                    String gcf = conf.get(ProjectConstant.GENRE_CUT_FINAL_CONF_PREFIX + gcfID);
                                    if (gcf != null) {
                                        tRecord.setGcf(gcf);
                                      //RM context.getCounter("Debug", "Set GCF on the level 2 of genre").increment(1);
                                    }
                                } else {
                                    gcfID = ProjectConstant.CONSTANT_UNFOUND_FLAG;
                                }
                            }
                        }
                        /*
                        If genre level 1 doesn't have the gcfID then try to get genre cut final for the genre level 2.
                        Because all the genre in the same hierarchy have the same gcf.
                         */
                        if (gcfID == null || gcfID.trim().length() == 0 || gcfID.equals(ProjectConstant.CONSTANT_UNFOUND_FLAG)) {
                            gn = genreLevelMap.get(3);
                            if (gn != null) {

                                gcfID = gn.getGcfID();
                                if (gcfID != null && gcfID.trim().length() != 0) {
                                    String gcf = conf.get(ProjectConstant.GENRE_CUT_FINAL_CONF_PREFIX + gcfID);
                                    if (gcf != null) {
                                        tRecord.setGcf(gcf);
                                      //RM context.getCounter("Debug", "Set GCF on the level 3 of genre").increment(1);
                                    }
                                } else {
                                    //Stop here. Make at most triple attempts to find the GCF
                                	//RM context.getCounter("Debug", "Failed to find GCF more than triple times").increment(1);
                                }
                            }
                        }
                    } else {
                        //genreLevelMap is null
                    	//RM context.getCounter("Debug", "Found genreLevelMap is null").increment(1);
                    }
                }
              //RM context.getCounter("Debug", "Set Genre Description and GCF").increment(1);
            } else {
            	//RM context.getCounter("Debug", "Found empty genreID in transaction").increment(1);
            }
            //Set user registered address info
            if (tRecord.getUserID().length() > 0) {
//            this.setRegisterAddr(tRecord);
                String buyerID = tRecord.getUserID();
                String valueStr;
                String addrCity = "";
                String addrRegion = "";
                String addrCountry = "";
                String addrZip = "";
                if (buyerID != null) {
                    try {
                        LongWritable userID = new LongWritable(Long.parseLong(buyerID));
                        Text value = new Text();
                        Writable val = userContactsMapFileReader.get(userID, value);
                      //RM context.getCounter("Debug", "Lookup user_contacts mapfile").increment(1);
                        Text temp = val != null ? (Text) val : null;
                        if (temp != null) {
                            valueStr = temp.toString();
                            String[] valueList = valueStr.split(ProjectConstant.DELIMITER);
                            if (valueList.length == 4) {
                                addrZip = valueList[0];
                                addrCountry = valueList[1];
                                addrRegion = valueList[2];
                                addrCity = valueList[3];
                            }
                        }
                    } catch (Exception e) {
                        //do nothing
                    }
                    tRecord.setRegCity(addrCity);
                    tRecord.setRegRegion(addrRegion);
                    tRecord.setRegCountry(addrCountry);
                    tRecord.setRegZipcode(addrZip);
                }
            } else {
            	//RM context.getCounter("Debug", "Invalid userID in transaction").increment(1);
            }
            //Finish information retrieve
            context.write(outputKey, tRecord);
        } else {
        	//RM context.getCounter("Debug", "Found transaction not existing in stub_trans").increment(1);
        }
    }

    //TODO refactor and move to individual util package
    String getEventDescFromMapFile(String eventID1) {
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

    //TODO refactor and move to individual util package
    String getGenreLevel1Description(String firstLevelGenreID) {
        String genreDescTemp = "";
        if (firstLevelGenreID != null && firstLevelGenreID.length() > 0) {
            if (genreTreeMap != null) {
                HashMap<Integer, GenreNode> genreLevelMap = genreTreeMap.get(firstLevelGenreID);
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

    @Deprecated
    void setGenreDescAndGCF(TransactionRecord tRecord) {
        String firstLevelGenreID = tRecord.getPurchaseGenreID();
        if (firstLevelGenreID != null && firstLevelGenreID.length() > 0) {
            if (genreTreeMap != null) {
                String gcfID = null;
                HashMap<Integer, GenreNode> genreLevelMap = genreTreeMap.get(firstLevelGenreID);
                if (genreLevelMap != null) {
                    String genreDescTemp;

                    GenreNode gn = genreLevelMap.get(1);
                    if (gn != null) {
                        genreDescTemp = gn.getGenreDescription();
                        tRecord.setPurchaseGenreDescLevel1(genreDescTemp);
                        /*
                        First try to get genre cut final for the genre level 1.
                        Because all the genre in the same hierarchy have the same gcf.
                         */
                        gcfID = gn.getGcfID();
                        if (gcfID != null && gcfID.trim().length() != 0) {
                            String gcf = conf.get(ProjectConstant.GENRE_CUT_FINAL_CONF_PREFIX + gcfID);
                            if (gcf != null) {
                                tRecord.setGcf(gcf);
                            }
                        } else {
                            gcfID = ProjectConstant.CONSTANT_UNFOUND_FLAG;
                        }
                    }
                        /*If genre level 1 doesn't have the gcfID then try to get genre cut final for the genre level 2.
                        Because all the genre in the same hierarchy have the same gcf.
                        */
                    if (gcfID == null || gcfID.trim().length() == 0 || gcfID.equals(ProjectConstant.CONSTANT_UNFOUND_FLAG)) {
                        gn = genreLevelMap.get(2);
                        if (gn != null) {

                            gcfID = gn.getGcfID();
                            if (gcfID != null && gcfID.trim().length() != 0) {
                                String gcf = conf.get(ProjectConstant.GENRE_CUT_FINAL_CONF_PREFIX + gcfID);
                                if (gcf != null) {
                                    tRecord.setGcf(gcf);
                                }
                            } else {
                                gcfID = ProjectConstant.CONSTANT_UNFOUND_FLAG;
                            }
                        }
                    }
                        /*
                        If genre level 1 doesn't have the gcfID then try to get genre cut final for the genre level 2.
                        Because all the genre in the same hierarchy have the same gcf.
                         */
                    if (gcfID == null || gcfID.trim().length() == 0 || gcfID.equals(ProjectConstant.CONSTANT_UNFOUND_FLAG)) {
                        gn = genreLevelMap.get(3);
                        if (gn != null) {

                            gcfID = gn.getGcfID();
                            if (gcfID != null && gcfID.trim().length() != 0) {
                                String gcf = conf.get(ProjectConstant.GENRE_CUT_FINAL_CONF_PREFIX + gcfID);
                                if (gcf != null) {
                                    tRecord.setGcf(gcf);
                                }
                            } else {
                                //Stop here. Make at most triple attempts to find the GCF
                            }
                        }
                    }
                } else {
                    //genreLevelMap is null
                }
            }

        }

    }

    void setRegisterAddr(TransactionRecord tRecord) {
        String buyerID = tRecord.getUserID();
        String valueStr;
        String addrCity = "";
        String addrRegion = "";
        String addrCountry = "";
        String addrZip = "";
        if (buyerID != null) {
            try {
                LongWritable userID = new LongWritable(Long.parseLong(buyerID));
                Text value = new Text();
                Writable val = userContactsMapFileReader.get(userID, value);
                Text temp = val != null ? (Text) val : null;
                if (temp != null) {
                    valueStr = temp.toString();
                    String[] valueList = valueStr.split(ProjectConstant.DELIMITER);
                    if (valueList.length == 4) {
                        addrCity = valueList[0];
                        addrRegion = valueList[1];
                        addrCountry = valueList[2];
                        addrZip = valueList[3];
                    }
                }
            } catch (Exception e) {
                //do nothing
            }
            tRecord.setRegCity(addrCity);
            tRecord.setRegRegion(addrRegion);
            tRecord.setRegCountry(addrCountry);
            tRecord.setRegZipcode(addrZip);
        }
    }


}
