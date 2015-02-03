package org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transaction.mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;
import org.willianzhao.omnitureanalysis.mapred.commons.model.BrowseBehaviorMetrics;
import org.willianzhao.omnitureanalysis.mapred.commons.model.BrowseHistory;
import org.willianzhao.omnitureanalysis.mapred.commons.model.CheckoutPageKey;
import org.willianzhao.omnitureanalysis.mapred.commons.model.GenreFactory;
import org.willianzhao.omnitureanalysis.mapred.commons.model.GenreNode;
import org.willianzhao.omnitureanalysis.mapred.commons.model.VisitStep;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.mapfile.EventsMapFileFactory;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transaction.CheckoutAnalyzer;

/**
 * Created by willianzhao on 5/4/14.
 */
public class BrowseHistoryReducer extends Reducer<Text, VisitStep, NullWritable, Text> {

    HashMap<String, HashMap<Integer, GenreNode>> genreTreeMap;
    Configuration conf;
    boolean outputDetails = false;

    SimpleDateFormat simpleFormatter;

    MapFile.Reader eventMapFileReader;

    private MultipleOutputs<NullWritable, Text> mos;

    //private Logger logger = LogManager.getLogger(BrowseHistoryMapper.class.getName());

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.conf = context.getConfiguration();
        String debugFlag = conf.get(ProjectConstant.PARAM_LABEL_DEBUG);
        if (debugFlag != null && debugFlag.equals(ProjectConstant.CONSTANT_TRUE)) {
            outputDetails = true;
            this.mos = new MultipleOutputs<NullWritable, Text>(context);
        }
        if (eventMapFileReader == null) {
            EventsMapFileFactory eventMapFile = null;
            try {
                eventMapFile = new EventsMapFileFactory(conf);
                eventMapFileReader = eventMapFile.getMapfileReader();
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
    protected void reduce(Text key, Iterable<VisitStep> values,
                          Context context) throws IOException, InterruptedException {
        NullWritable metricsKey = NullWritable.get();
        Text metricValue = new Text();
        NullWritable detailsKey = NullWritable.get();
        Text detailsValue = new Text();
        String browseStr = "";
        CheckoutAnalyzer ca = new CheckoutAnalyzer(context, values, genreTreeMap, eventMapFileReader, simpleFormatter);
        ArrayList<CheckoutPageKey> checkoutRecs = ca.getCheckoutRecs();
        int checkCnt = checkoutRecs.size();
        if (checkCnt == 0) {
            return;
        }
        HashSet<String> ticketSet = new HashSet<String>();
        HashSet<String> transactionSet = new HashSet<String>();
        for (CheckoutPageKey checkoutRec : checkoutRecs) {
            String ticketID = checkoutRec.getPurchaseTicketID();
            String transactionID = checkoutRec.getTransactionID();
            if (transactionID.length() > 0 && !transactionSet.contains(transactionID)) {
                transactionSet.add(transactionID);
                ticketSet.add(ticketID);
              //RM context.getCounter("Analysis Statistic", "Un-duplicated transaction count ").increment(1);

            } else {
                if (transactionID.length() == 0) {
                    // transaction id is unavailable then look for ticket id
                    if (!ticketSet.contains(ticketID)) {
                        ticketSet.add(ticketID);
                      //RM  context.getCounter("Analysis Statistic", "Un-duplicated transaction count ").increment(1);
                    } else {
                    	//RM context.getCounter("Debug", "Removed duplicate transaction count ").increment(1);
                        continue;
                    }
                } else {
                    // we have processed this transaction then skip it
                	//RM context.getCounter("Debug", "Removed duplicate transaction count ").increment(1);
                    continue;
                }
            }
            String purchaseTime = checkoutRec.getPurchaseTime();
            ArrayList<BrowseHistory> browseRecs = ca.getFootprintsBeforeCheckout(purchaseTime);
            if (browseRecs.size() > 0) {
            	//RM context.getCounter("Analysis Statistic", "No# of Total : 2a + 2b + 2c").increment(1);
            }
            if (outputDetails) {
                //Construct the browse list string
                browseStr = "[";
                if (browseRecs.size() > 0) {
                    Iterator<BrowseHistory> brItr = browseRecs.iterator();
                    while (brItr.hasNext()) {
                        BrowseHistory br = brItr.next();
                        browseStr = browseStr + br.toString();
                        if (brItr.hasNext()) {
                            browseStr = browseStr + ProjectConstant.DELIMITER;
                        }

                    }

                }
                browseStr = browseStr + "]";
            }
            BrowseBehaviorMetrics bbm = ca.analyzeBrowseBehaviorForCheckout(checkoutRec, browseRecs);
            if (bbm.getUsmwebFlag2a().equals("1")) {
            	//RM context.getCounter("Analysis Statistic", "No# of 2a : US_MWEB").increment(1);
            } else if (bbm.getUsmwebFlag2b().equals("1")) {
            	//RM context.getCounter("Analysis Statistic", "No# of 2b : US_MWEB").increment(1);
            }
            if (bbm.getIpadFlag2a().equals("1")) {
            	//RM context.getCounter("Analysis Statistic", "No# of 2a : IPAD_APPS").increment(1);
            } else if (bbm.getIpadFlag2b().equals("1")) {
            	//RM context.getCounter("Analysis Statistic", "No# of 2b : IPAD_APPS").increment(1);
            }
            if (bbm.getMobileFlag2a().equals("1")) {
            	//RM context.getCounter("Analysis Statistic", "No# of 2a : MOBILE_APPS").increment(1);
            } else if (bbm.getMobileFlag2b().equals("1")) {
            	//RM context.getCounter("Analysis Statistic", "No# of 2b : MOBILE_APPS").increment(1);
            }
            if (bbm.getFlag2a().equals("1")) {
            	//RM context.getCounter("Analysis Statistic", "No# of 2a").increment(1);
            }
            if (bbm.getFlag2b().equals("1")) {
            	//RM context.getCounter("Analysis Statistic", "No# of 2b").increment(1);
            }
            if (bbm.getFlag2c().equals("1")) {
            	//RM context.getCounter("Analysis Statistic", "No# of 2c").increment(1);
            }
            if (bbm.getFlagOthers().equals("1")) {
            	//RM context.getCounter("Analysis Statistic", "No# of Others (no mobile browse)").increment(1);
            }
            String analysisResult = bbm.toString();
            metricValue.set(analysisResult);
            context.write(metricsKey, metricValue);
            if (outputDetails) {
                String checkoutStr = checkoutRec.toString();
                String detailsStr = checkoutStr + ProjectConstant.DELIMITER + browseStr;
                detailsValue.set(detailsStr);
                mos.write(ProjectConstant.TASK_CHECKOUT_BROWSEDETAIL, detailsKey, detailsValue);

            }
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
