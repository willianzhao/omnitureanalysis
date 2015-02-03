package org.willianzhao.omnitureanalysis.mapred.commons.model;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by willianzhao on 5/4/14.
 */
public class CheckoutPageKey extends GenreTreeAbstract implements WritableComparable {

    String userID;
    String buyerRegisterTime ="";
    String purchaseTime = "";
    String purchaseEventID = "";
    String purchaseGenreID = "";
    String purchaseGenreDesc = "";
    String purchaseTicketID = "";
    String transactionID = "";

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

    String purchaseEventDesc = "";

    String top2ndGenreDesc ="";

    String gcf="";

    public CheckoutPageKey() {
    }

    public CheckoutPageKey(String userID,  String buyerRegisterTime, String purchaseTime, String purchaseEventID, String genreID, String purchaseTicketID, String transactionID) {
        this.userID = userID;
        this.buyerRegisterTime = buyerRegisterTime;
        this.purchaseTime = purchaseTime;
        this.purchaseEventID = purchaseEventID;
        this.purchaseGenreID = genreID;
        this.purchaseTicketID = purchaseTicketID;
        this.transactionID = transactionID;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }
    public String getBuyerRegisterTime() {
        return buyerRegisterTime;
    }

    public void setBuyerRegisterTime(String buyerRegisterTime) {
        this.buyerRegisterTime = buyerRegisterTime;
    }
    public String getPurchaseTime() {
        return purchaseTime;
    }

    public void setPurchaseTime(String purchaseTime) {
        this.purchaseTime = purchaseTime;
    }

    public String getPurchaseEventID() {
        return purchaseEventID;
    }

    public void setPurchaseEventID(String purchaseEventID) {
        this.purchaseEventID = purchaseEventID;
    }

    public String getPurchaseGenreDesc() {
        return purchaseGenreDesc;
    }

    public void setPurchaseGenreDesc(String purchaseGenreDesc) {
        this.purchaseGenreDesc = purchaseGenreDesc;
    }

    public String getPurchaseGenreID() {
        return purchaseGenreID;
    }

    public void setPurchaseGenreID(String purchaseGenreID) {
        this.purchaseGenreID = purchaseGenreID;
    }

    public String getPurchaseTicketID() {
        return purchaseTicketID;
    }

    public void setPurchaseTicketID(String purchaseTicketID) {
        this.purchaseTicketID = purchaseTicketID;
    }

    public String getTransactionID() {
        return transactionID;
    }

    public void setTransactionID(String transactionID) {
        this.transactionID = transactionID;
    }

    public String getLevel2GenreID() {
        return level2GenreID;
    }

    public void setLevel2GenreID(String level2GenreID) {
        this.level2GenreID = level2GenreID;
    }

    @Override
    public void setLevel1GenreID(String levelGenreID) {
        this.purchaseGenreID = levelGenreID;
    }

    @Override
    public void setLevel1GenreDesc(String levelGenreDesc) {
        this.purchaseGenreDesc = levelGenreDesc;
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

    public String getPurchaseEventDesc() {
        return purchaseEventDesc;
    }

    public void setPurchaseEventDesc(String purchaseEventDesc) {
        this.purchaseEventDesc = purchaseEventDesc;
    }

    public String getTop2ndGenreDesc() {

        if(level4GenreID.trim().length()==0){
            //There are only 3 levels of genre hierarchy
            top2ndGenreDesc=level3GenreDesc;
        }else if(level5GenreID.trim().length()>0 && level6GenreID.trim().length()==0){
            //There are more than 6 levels of genre hierarchy
            top2ndGenreDesc=level3GenreDesc;
        }else if (level6GenreID.trim().length()>0){
            //Give a default value
            top2ndGenreDesc=level4GenreDesc;
        }else{
            top2ndGenreDesc=level3GenreDesc;
        }

        return top2ndGenreDesc;
    }

    public String getGcf() {
        return gcf;
    }

    public void setGcf(String gcf) {
        this.gcf = gcf;
    }

    @Override
    public String toString() {
        return "[{" +
                "userID='" + userID + '\'' +
                ", buyerRegisterTime='" + buyerRegisterTime + '\'' +
                ", purchaseTime='" + purchaseTime + '\'' +
                ", purchaseEventID='" + purchaseEventID + '\'' +
                ", purchaseEventDesc='" + purchaseEventDesc + '\'' +
                ", purchaseGenreID='" + purchaseGenreID + '\'' +
                ", purchaseGenreDesc='" + purchaseGenreDesc + '\'' +
                ", purchaseTicketID='" + purchaseTicketID + '\'' +
                ", transactionID='" + transactionID + '\'' +
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
                ", GenreCutFinal='" + gcf + '\'' +
                "}]";
    }

    public String getBusinessKey() {
        return "{" +
                "userID='" + userID + '\'' +
                ", transactionID='" + transactionID + '\'' +
                ", purchaseTime='" + purchaseTime + '\'' +
                ", purchaseEventID='" + purchaseEventID + '\'' +
                ", purchaseGenreID='" + purchaseGenreID + '\'' +
                ", purchaseGenreDesc='" + purchaseGenreDesc + '\'' +
                "}";
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof CheckoutPageKey) {
            CheckoutPageKey other = (CheckoutPageKey) o;
            return this.getBusinessKey().compareTo(other.getBusinessKey());
        } else {
            return 1;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, userID);
        Text.writeString(out, buyerRegisterTime);
        Text.writeString(out, purchaseTime);
        Text.writeString(out, purchaseEventID);
        Text.writeString(out, purchaseEventDesc);
        Text.writeString(out, purchaseGenreID);
        Text.writeString(out, purchaseGenreDesc);
        Text.writeString(out, purchaseTicketID);
        Text.writeString(out, transactionID);
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
        userID = Text.readString(in);
        buyerRegisterTime = Text.readString(in);
        purchaseTime = Text.readString(in);
        purchaseEventID = Text.readString(in);
        purchaseEventDesc = Text.readString(in);
        purchaseGenreID = Text.readString(in);
        purchaseGenreDesc = Text.readString(in);
        purchaseTicketID = Text.readString(in);
        transactionID = Text.readString(in);
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
        return this.toString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        int result = this.compareTo(o);
        return result == 0;
    }

	
}
