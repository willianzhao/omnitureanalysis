package org.willianzhao.omnitureanalysis.mapred.commons.dm;

import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by willianzhao on 5/3/14.
 */
public class GenreIdentifier extends SmartIdentifier {
    Path genreFile;
    String ticketID;
    String eventID;
    String genreID;

	private static Logger logger = LoggerFactory.getLogger(GenreIdentifier.class);

    public GenreIdentifier(Configuration config, String[] inputFields) {
        super(config, inputFields);
    }

    public GenreIdentifier(Configuration config, Path genreFile, String[] inputFields) {
        super(config, inputFields);
        this.genreFile = genreFile;
    }

    /*
    When the field is not found, there should be no error report but give a default empty string.
     */
    public String getEventID() {

        if (eventID == null) {
            String product_list;
            int position = 0;
            try {
                String product_list1 = conf.get("PRODUCT_LIST");
                if (product_list1 != null) {
                    position = Integer.parseInt(product_list1);
                    product_list = rawFields[position].trim();
                    if (product_list.length() > 1) {
                        String[] productList = product_list.split(";");
                        if (productList.length > 1) {
                            eventID = productList[1].trim();
                        } else {
                            eventID = product_list.trim();
                        }
                        // check if the eventID is digital
                        int length = eventID.length();
                        if (length > 0 && length < ProjectConstant.PATTERN_EVENTID_STR_MAXLENTTH) {
                            try {
                                int i = Integer.parseInt(eventID);
                            } catch (NumberFormatException cce) {
                                // The eventID has non-digits then give it a default empty string
                                System.out.println("product_list="+product_list+"\t"+"eventID::"+eventID);
                                eventID = "";
                            }
                        } else {
                            eventID = "";
                        }
                    }
                } else {
                    logger.error("The PRODUCT_LIST is not defined in conf file");
                }
            } catch (Exception aie) {
                // do nothing
            }

        }

        /*
        Return the eventID as null or other values
         */
        return eventID;
    }
    public String getGenreID() {
        if (genreID == null) {
            int position = 0;
            try {
                String post_prop70 = conf.get("POST_PROP70");
                if (post_prop70 != null) {
                    position = Integer.parseInt(post_prop70);
                    genreID = rawFields[position].trim();
                    int length = genreID.length();
                    if (length == 0) {
                        position = Integer.parseInt(conf.get("PROP70"));
                        genreID = rawFields[position].trim();
                        length = genreID.length();
                        if (length > 0 && length < ProjectConstant.PATTERN_GENREID_STR_MAXLENTTH) {
                            try {
                                int i = Integer.parseInt(genreID);
                            } catch (NumberFormatException cce) {
                                //The genreID has non-digits then give it a default empty string
                                //do nothing
                            }
                        }
                    }
                } else {
                    logger.error("The POST_PROP70 is not defined in conf file");
                }
            } catch (Exception aie) {
                // do nothing
            }

        }
        return genreID;
    }

    public String getTicketIDFromURL() {
        ticketID = "";
        int position = 0;
        try {
            position = Integer.parseInt(conf.get("PAGE_URL"));
            String localPageURL = rawFields[position].trim();
            if (localPageURL.length() > 0) {

                    /*
                    indexOf() will be much faster than pattern match
                     */
                position = localPageURL.indexOf(ProjectConstant.PATTERN_PREFIX_TICKETID_STR);
                if (position > 0) {
                    int urlLength = localPageURL.length();
                    int beginIndex = position + ProjectConstant.PATTERN_PREFIX_TICKETID_STR_LENTTH + 1;
                    String part;
                    int endIndex = beginIndex + ProjectConstant.PATTERN_TICKETID_STR_MAXLENTTH;
                    if (endIndex > urlLength) {
                        part = localPageURL.substring(beginIndex);
                    } else {
                        part = localPageURL.substring(beginIndex, endIndex);
                    }
                    try {
                        int i = Integer.parseInt(part);
                        //if exception happens, then assign the substring to ticketid
                        ticketID = part;
                    } catch (NumberFormatException cce) {
                        for (int i = part.length() - 1; i >= ProjectConstant.PATTERN_TICKETID_STR_MINLENTTH; i--) {
                            final char c = part.charAt(i);
                            if (c > ProjectConstant.CONSTANT_ASCII_DIGIT_START_POS && c < ProjectConstant.CONSTANT_ASCII_DIGIT_END_POS) {
                                ticketID = part.substring(0, i + 1);
                                break;
                            }
                        }
                    }
                }
            }
        } catch (Exception aie) {
            // do nothing
        }
        return ticketID;
    }

    public String getTicketIDFromURL(Pattern pattern) {
        //Get initial value. Make sure it's not null
        ticketID = "";
        int position = 0;
        try {
            position = Integer.parseInt(conf.get("PAGE_URL"));
            String localPageURL = rawFields[position].trim();
            if (localPageURL.length() > 0) {

                    /*
                    The regex match is very expensive. Avoid use this as much as you can.
                     */
                Matcher matcher = pattern.matcher(localPageURL);
                if (matcher.find()) {
                    String found = matcher.group();
                    int tempLoc = found.indexOf("=");
                    if (tempLoc != -1) {
                        ticketID = found.substring(tempLoc + 1);
                    }

                }

            }
        } catch (Exception aie) {
            // do nothing
        }
        return ticketID;
    }

    public String getEventIDFromMapFile(MapFile.Reader reader, String ticketID1) {
        if (eventID == null || eventID.trim().length() == 0) {
            try {
                LongWritable ticketKey = new LongWritable(Long.parseLong(ticketID1));
                LongWritable eventValue = new LongWritable();
                Writable val = reader.get(ticketKey, eventValue);
                LongWritable temp = val != null ? (LongWritable) val : null;
                if (temp != null) {
                    eventID = temp.toString();
                } else {
                    eventID = "";
                }
            } catch (Exception e) {
                eventID = "";
            }

        }
        return eventID;
    }

    public String getGenreIDFromMapFile(MapFile.Reader reader, String eventID1) {
        if (eventID1 != null && (genreID == null || genreID.trim().length() == 0)) {
            try {
                LongWritable eventKey = new LongWritable(Long.parseLong(eventID1));
                LongWritable genreValue = new LongWritable();
                Writable val = reader.get(eventKey, genreValue);
                LongWritable temp = val != null ? (LongWritable) val : null;
                if (temp != null) {
                    genreID = temp.toString();
                } else {
                    genreID = "";
                }
            } catch (Exception e) {
                genreID = "";
                logger.trace("Something wrong in mapfile lookup");
                //e.printStackTrace(logger.getStream(Level.TRACE));
            }

        }
        return genreID;
    }
}
