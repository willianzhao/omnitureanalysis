package org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.transaction.mapreduce;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;
import org.willianzhao.omnitureanalysis.mapred.commons.model.StubTransFactory;
import org.willianzhao.omnitureanalysis.mapred.commons.model.VisitStep;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.mapfile.EventsMapFileFactory;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.mapfile.UserMapFileFactory;

/**
 * Created by willianzhao on 6/10/14.
 */
public class StubTransMapper extends Mapper<LongWritable, Text, Text, VisitStep> {

    Configuration conf = null;

    StubTransFactory stubTransFactory;

    MapFile.Reader eventMapFileReader;
    MapFile.Reader userMapFileReader;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.conf = context.getConfiguration();
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
        if (userMapFileReader == null) {
            UserMapFileFactory userMapFile = null;
            try {
                userMapFile = new UserMapFileFactory(conf);
                userMapFileReader = userMapFile.getMapfileReader();

            } catch (ParseException e) {
                //e.printStackTrace(logger.getStream(Level.ERROR));
            } catch (Exception e) {
                //e.printStackTrace(logger.getStream(Level.ERROR));
            }

        }
        String mode =conf.get(ProjectConstant.PARAM_LABEL_MODE);
        try {
            stubTransFactory = new StubTransFactory(mode,context, conf, eventMapFileReader, userMapFileReader);
        } catch (ParseException e) {
            //e.printStackTrace(logger.getStream(Level.ERROR));
        }

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (stubTransFactory != null) {
            String[] rawFields = value.toString().split(ProjectConstant.CONTROLA_REC_DELIMITER);
            VisitStep transStep = stubTransFactory.getVisitStep(rawFields);
            String actionType = transStep.getActionType();
            if (!actionType.equals(ProjectConstant.ACTION_NONEED)) {
                String transactionID = transStep.getTransactionID();
                context.write(new Text(transactionID), transStep);
            }
        }
    }
}
