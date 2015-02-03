package org.willianzhao.omnitureanalysis.mapred.commons.model;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by willianzhao on 5/4/14.
 */
public class BrowseHistory extends GenreTreeAbstract implements WritableComparable {

    String userID;
    String visitTime;
    String ticketID;
    String eventID;
    String level1GenreID = "";
    String level1GenreDesc = "";
    String level2GenreID = "";
    String level2GenreDesc = "";
    String level3GenreID = "";
    String level3GenreDesc = "";
    String level4GenreID = "";
    String level4GenreDesc = "";
    String level5GenreID = "";
    String level5GenreDesc = "";
    String level6GenreID = "";
    String level6GenreDesc = "";
    String dataSource = "";
    String gcf = "";

    public BrowseHistory(String dataSource, String userID, String visitTime, String ticketID, String eventID, String level1GenreID) {
        this.dataSource = dataSource;
        this.userID = userID;
        this.visitTime = visitTime;
        this.ticketID = ticketID;
        this.eventID = eventID;
        this.level1GenreID = level1GenreID;
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

    public String getLevel1GenreID() {
        return level1GenreID;
    }

    public void setLevel1GenreID(String level1GenreID) {
        this.level1GenreID = level1GenreID;
    }

    public String getLevel1GenreDesc() {
        return level1GenreDesc;
    }

    public void setLevel1GenreDesc(String level1GenreDesc) {
        this.level1GenreDesc = level1GenreDesc;
    }

    public String getLevel2GenreID() {
        return level2GenreID;
    }

    public void setLevel2GenreID(String level2GenreID) {
        this.level2GenreID = level2GenreID;
    }

    public String getLevel2GenreDesc() {
        return level2GenreDesc;
    }

    public void setLevel2GenreDesc(String level2GenreDesc) {
        this.level2GenreDesc = level2GenreDesc;
    }

    public String getLevel3GenreID() {
        return level3GenreID;
    }

    public void setLevel3GenreID(String level3GenreID) {
        this.level3GenreID = level3GenreID;
    }

    public String getLevel3GenreDesc() {
        return level3GenreDesc;
    }

    public void setLevel3GenreDesc(String level3GenreDesc) {
        this.level3GenreDesc = level3GenreDesc;
    }

    public String getLevel4GenreID() {
        return level4GenreID;
    }

    public void setLevel4GenreID(String level4GenreID) {
        this.level4GenreID = level4GenreID;
    }

    public String getLevel4GenreDesc() {
        return level4GenreDesc;
    }

    public void setLevel4GenreDesc(String level4GenreDesc) {
        this.level4GenreDesc = level4GenreDesc;
    }

    public String getLevel5GenreID() {
        return level5GenreID;
    }

    public void setLevel5GenreID(String level5GenreID) {
        this.level5GenreID = level5GenreID;
    }

    public String getLevel5GenreDesc() {
        return level5GenreDesc;
    }

    public void setLevel5GenreDesc(String level5GenreDesc) {
        this.level5GenreDesc = level5GenreDesc;
    }

    public String getLevel6GenreID() {
        return level6GenreID;
    }

    public void setLevel6GenreID(String level6GenreID) {
        this.level6GenreID = level6GenreID;
    }

    public String getLevel6GenreDesc() {
        return level6GenreDesc;
    }

    public void setLevel6GenreDesc(String level6GenreDesc) {
        this.level6GenreDesc = level6GenreDesc;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public String getGcf() {
        return gcf;
    }

    public void setGcf(String gcf) {
        this.gcf = gcf;
    }

    @Override
    public String toString() {
        return "{" +
                "dataSource='" + dataSource + '\'' +
                ", userID='" + userID + '\'' +
                ", visitTime='" + visitTime + '\'' +
                ", ticketID='" + ticketID + '\'' +
                ", eventID='" + eventID + '\'' +
                ", level1GenreID='" + level1GenreID + '\'' +
                ", level1GenreDesc='" + level1GenreDesc + '\'' +
                ", level2GenreID='" + level2GenreID + '\'' +
                ", level2GenreDesc='" + level2GenreDesc + '\'' +
                ", level3GenreID='" + level3GenreID + '\'' +
                ", level3GenreDesc='" + level3GenreDesc + '\'' +
                ", level4GenreID='" + level4GenreID + '\'' +
                ", level4GenreDesc='" + level4GenreDesc + '\'' +
                ", level5GenreID='" + level5GenreID + '\'' +
                ", level5GenreDesc='" + level5GenreDesc + '\'' +
                ", level6GenreID='" + level6GenreID + '\'' +
                ", level6GenreDesc='" + level6GenreDesc + '\'' +
                ", gcf='" + gcf + '\'' +
                "}";
    }

    public String getBusinessKey() {
        return "dataSource='" + dataSource + '\'' +
                ", userID='" + userID + '\'' +
                ", visitTime='" + visitTime + '\'' +
                ", ticketID='" + ticketID + '\'' +
                ", eventID='" + eventID + '\'' +
                ", level1GenreID='" + level1GenreID;
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof BrowseHistory) {
            BrowseHistory other = (BrowseHistory) o;
            return this.getBusinessKey().compareTo(other.getBusinessKey());
        } else {
            return 1;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, dataSource);
        Text.writeString(out, userID);
        Text.writeString(out, visitTime);
        Text.writeString(out, ticketID);
        Text.writeString(out, eventID);
        Text.writeString(out, level1GenreID);
        Text.writeString(out, level1GenreDesc);
        Text.writeString(out, level2GenreID);
        Text.writeString(out, level2GenreDesc);
        Text.writeString(out, level3GenreID);
        Text.writeString(out, level3GenreDesc);
        Text.writeString(out, level4GenreID);
        Text.writeString(out, level4GenreDesc);
        Text.writeString(out, level5GenreID);
        Text.writeString(out, level5GenreDesc);
        Text.writeString(out, level6GenreID);
        Text.writeString(out, level6GenreDesc);
        Text.writeString(out, gcf);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        dataSource = Text.readString(in);
        userID = Text.readString(in);
        visitTime = Text.readString(in);
        ticketID = Text.readString(in);
        eventID = Text.readString(in);
        level1GenreID = Text.readString(in);
        level1GenreDesc = Text.readString(in);
        level2GenreID = Text.readString(in);
        level2GenreDesc = Text.readString(in);
        level3GenreID = Text.readString(in);
        level3GenreDesc = Text.readString(in);
        level4GenreID = Text.readString(in);
        level4GenreDesc = Text.readString(in);
        level5GenreID = Text.readString(in);
        level5GenreDesc = Text.readString(in);
        level6GenreID = Text.readString(in);
        level6GenreDesc = Text.readString(in);
        gcf = Text.readString(in);

    }

    @Override
    public int hashCode() {
        return this.getBusinessKey().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        int result = this.compareTo(o);
        return result == 0;
    }
}
