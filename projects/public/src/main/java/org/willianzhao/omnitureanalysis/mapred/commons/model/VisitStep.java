package org.willianzhao.omnitureanalysis.mapred.commons.model;

import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by willianzhao on 5/4/14.
 */
public class VisitStep implements WritableComparable {

    String userID = "";
    String visitTime = "";
    String transactionID = "";
    String ticketID = "";
    String eventID = "";
    String genreID = "";
    String dataSource = "";
    String actionType = "";

    String buyerID = "";
    String buyerRegisterTime = "";

    public VisitStep() {
    }

    public VisitStep(String userID, String visitTime, String ticketID, String eventID, String genreID, String dataSource, String actionType, String buyerRegisterTime) {
        this.userID = userID;
        this.visitTime = visitTime;
        this.ticketID = ticketID;
        this.eventID = eventID;
        this.genreID = genreID;
        this.dataSource = dataSource;
        this.actionType = actionType;
	this.buyerRegisterTime = buyerRegisterTime;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public String getVisitTime() {
        return visitTime;
    }

    public void setVisitTime(String visitTime) {
        this.visitTime = visitTime;
    }

    public String getTicketID() {
        return ticketID;
    }

    public void setTicketID(String ticketID) {
        this.ticketID = ticketID;
    }

    public String getEventID() {
        return eventID;
    }

    public void setEventID(String eventID) {
        this.eventID = eventID;
    }

    public String getGenreID() {
        return genreID;
    }

    public void setGenreID(String genreID) {
        this.genreID = genreID;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public String getActionType() {
        return actionType;
    }

    public void setActionType(String actionType) {
        this.actionType = actionType;
    }

    public String getTransactionID() {
        return transactionID;
    }

    public void setTransactionID(String transactionID) {
        this.transactionID = transactionID;
    }

    public String getBuyerID() {
        return buyerID;
    }

    public void setBuyerID(String buyerID) {
        this.buyerID = buyerID;
    }

    public String getBuyerRegisterTime() {
        return buyerRegisterTime;
    }

    public void setBuyerRegisterTime(String buyerRegisterTime) {
        this.buyerRegisterTime = buyerRegisterTime;
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof VisitStep) {
            VisitStep other = (VisitStep) o;
            return this.toString().compareTo(other.toString());
        } else {
            return 1;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, userID);
        Text.writeString(out, visitTime);
        Text.writeString(out, transactionID);
        Text.writeString(out, ticketID);
        Text.writeString(out, eventID);
        Text.writeString(out, genreID);
        Text.writeString(out, dataSource);
        Text.writeString(out, actionType);
        Text.writeString(out, buyerID);
        Text.writeString(out, buyerRegisterTime);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.userID = Text.readString(in);
        this.visitTime = Text.readString(in);
        this.transactionID = Text.readString(in);
        this.ticketID = Text.readString(in);
        this.eventID = Text.readString(in);
        this.genreID = Text.readString(in);
        this.dataSource = Text.readString(in);
        this.actionType = Text.readString(in);
        this.buyerID = Text.readString(in);
        this.buyerRegisterTime = Text.readString(in);
    }

    @Override
    public String toString() {
        return userID + ProjectConstant.DELIMITER + visitTime + ProjectConstant.DELIMITER
                + transactionID + ProjectConstant.DELIMITER
                + ticketID + ProjectConstant.DELIMITER + eventID + ProjectConstant.DELIMITER
                + genreID + ProjectConstant.DELIMITER + dataSource + ProjectConstant.DELIMITER
                + actionType + ProjectConstant.DELIMITER + buyerID + ProjectConstant.DELIMITER
                + buyerRegisterTime
                ;
    }
}
