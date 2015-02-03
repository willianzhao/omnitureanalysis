package org.willianzhao.omnitureanalysis.mapred.commons.model;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.willianzhao.omnitureanalysis.mapred.commons.dm.GenreIdentifier;
import org.willianzhao.omnitureanalysis.mapred.commons.dm.LocationIdentifier;
import org.willianzhao.omnitureanalysis.mapred.commons.dm.PageIdentifier;
import org.willianzhao.omnitureanalysis.mapred.commons.dm.TimeIdentifier;
import org.willianzhao.omnitureanalysis.mapred.commons.dm.UserIdentifier;
import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;

/**
 * Created by willianzhao on 5/8/14.
 */
public class BehaviorStudyFactory extends MRBaseFactory {


    public BehaviorStudyFactory(Configuration conf, Path inputSplitPath) throws Exception {
        super(conf, inputSplitPath);
    }

    public VisitStep reloadVisitStep(String[] rawFields) {
        /*
        Reload the visitstep string to visitstep object
         */
        VisitStep step = new VisitStep();
        if (rawFields.length == 10) {
            step.setUserID(rawFields[0]);
            step.setVisitTime(rawFields[1]);
            step.setTransactionID(rawFields[2]);
            step.setTicketID(rawFields[3]);
            step.setEventID(rawFields[4]);
            step.setGenreID(rawFields[5]);
            step.setDataSource(rawFields[6]);
            step.setActionType(rawFields[7]);
            step.setBuyerID(rawFields[8]);
            step.setBuyerRegisterTime(rawFields[9]);
        }
        return step;
    }

    public VisitStep getVisitStep(String[] rawFields) {
        PageIdentifier pageIden = new PageIdentifier(conf, rawFields);
        String actionType;
        if (dataSource.equals(ProjectConstant.SOURCE_US_WEB) || dataSource.equals(ProjectConstant.SOURCE_UK_WEB)) {
            if (pageIden.isPCCheckoutRecord()) {
                actionType = ProjectConstant.ACTION_CHECKOUT;

            } else {
                actionType = ProjectConstant.ACTION_NONEED;
            }

        } else if (!dataSource.equals(ProjectConstant.SOURCE_OTHERS)) {
            actionType = ProjectConstant.ACTION_BROWSE;

        } else {
            actionType = ProjectConstant.ACTION_NONEED;
        }
        String userID;
        String visitTime;
        String eventID;
        String genreID;
        String ticketID;
        String transactionID;
        VisitStep step = new VisitStep();
        if (!actionType.equals(ProjectConstant.ACTION_NONEED)) {
            UserIdentifier uIden = new UserIdentifier(conf, rawFields);
            LocationIdentifier locIden = new LocationIdentifier(conf, rawFields);
            TimeIdentifier timeIden = new TimeIdentifier(conf, rawFields);
            GenreIdentifier genreIden = new GenreIdentifier(conf, rawFields);
            //use getUserID()
            userID = uIden.getUserID();
            visitTime = timeIden.getVisitTime();
            if (actionType.equals(ProjectConstant.ACTION_CHECKOUT)) {
                transactionID = pageIden.getTransactionID();
                ticketID = genreIden.getTicketIDFromURL();
            } else {
                transactionID = "";
                ticketID = "";
            }
            eventID = genreIden.getEventID();
            genreID = genreIden.getGenreID();
            if (eventID == null) {
                eventID = "";
            }
            if (genreID == null) {
                genreID = "";
            }
            step.setUserID(userID);
            step.setActionType(actionType);
            step.setDataSource(dataSource);
            step.setEventID(eventID);
            step.setGenreID(genreID);
            step.setTicketID(ticketID);
            step.setVisitTime(visitTime);
            step.setTransactionID(transactionID);

        } else {
            step.setActionType(actionType);
            step.setDataSource(dataSource);

        }
        return step;
    }
}
