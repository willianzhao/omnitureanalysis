package org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transaction.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.willianzhao.omnitureanalysis.mapred.commons.dm.UserIdentifier;
import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;
import org.willianzhao.omnitureanalysis.mapred.commons.model.BehaviorStudyFactory;
import org.willianzhao.omnitureanalysis.mapred.commons.model.VisitStep;

/**
 * Created by willianzhao on 5/4/14.
 */
public class BrowseHistoryMapper extends Mapper<LongWritable, Text, Text, VisitStep> {

    int threshhold;
    int position;
    String exclude_hit = "";
    Configuration conf = null;
    BehaviorStudyFactory behaviorStudyFactory;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.conf = context.getConfiguration();
        threshhold = Integer.parseInt(ProjectConstant.VALID_REC_THRESHHOLD);
        Path filePath = ((FileSplit) context.getInputSplit()).getPath();
        try {
            behaviorStudyFactory = new BehaviorStudyFactory(conf, filePath);
        } catch (Exception e) {
            behaviorStudyFactory = null;
        }
        position = Integer.parseInt(conf.get("EXCLUDE_HIT"));

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] rawFields;
        if (behaviorStudyFactory != null) {
            String dataSource = behaviorStudyFactory.getDataSource();
            if (dataSource.equals(ProjectConstant.SOURCE_STUB_TRANS)) {

                /*
                The data is coming from Stub_Trans feed data.
                We need to de-serialize the stub_trans feed data to visitstep object.
                 */
                rawFields = value.toString().split(ProjectConstant.DELIMITER);
                VisitStep step = behaviorStudyFactory.reloadVisitStep(rawFields);
                Text userID = new Text(step.getUserID());
                context.write(userID, step);

            } else {

                /*
                The data is coming from Omniture log
                 */
                rawFields = value.toString().split(ProjectConstant.CONSTANT_HITDATA_DELIMITER);
                int fieldsLength = rawFields.length;
                if (fieldsLength > threshhold) {
                    exclude_hit = rawFields[position];
                    if (exclude_hit.trim().equals("0")) {
                        UserIdentifier uIden = new UserIdentifier(conf, rawFields);
                        String commUserID = uIden.getUserID().trim();
                        if (commUserID.length() > 0) {
                            Text userID = new Text();
                            userID.set(commUserID);
                            //Start to process the qualified records
                            VisitStep step = behaviorStudyFactory.getVisitStep(rawFields);
                            String actionType = step.getActionType();
                            if (!actionType.equals(ProjectConstant.ACTION_NONEED)) {
                                context.write(userID, step);

                            }

                        } // non-empty userid condition meet

                    } // exclude_hit condition meet

                } // the omniture record has enough fields

            }
        } else {
        	//RM context.getCounter("Debug", "Records fail to output due to null behaviorStudyFactory").increment(1);

        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
