package org.willianzhao.omnitureanalysis.mapred.commons.model;

import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by willianzhao on 5/9/14.
 */
public class BrowseBehaviorMetrics implements Writable {

    String userID;
    int isNewUser; //1 new user, 0 repeated user
    String purchaseTime;
    String ticketID;
    String transactionID;
    String purchaseEventID;
    String purchaseEventDesc;
    String purchaseGenreID;
    String purchaseGenreDescLevel1;
    int usmwebmatchEventCnt = -1;
    int usmwebmatchLevel1GenreCnt = -1;
    int usmwebmatchLevel2GenreCnt = -1;
    int usmwebmatchLevel3GenreCnt = -1;
    int usmwebmatchLevel4GenreCnt = -1;
    int usmwebmatchLevel5GenreCnt = -1;

    int ipadmatchEventCnt = -1;
    int ipadmatchLevel1GenreCnt = -1;
    int ipadmatchLevel2GenreCnt = -1;
    int ipadmatchLevel3GenreCnt = -1;
    int ipadmatchLevel4GenreCnt = -1;
    int ipadmatchLevel5GenreCnt = -1;

    int mobilematchEventCnt = -1;
    int mobilematchLevel1GenreCnt = -1;
    int mobilematchLevel2GenreCnt = -1;
    int mobilematchLevel3GenreCnt = -1;
    int mobilematchLevel4GenreCnt = -1;
    int mobilematchLevel5GenreCnt = -1;

    String usmwebFlag2a = "0";
    String usmwebFlag2b = "0";

    String ipadFlag2a = "0";
    String ipadFlag2b = "0";

    String mobileFlag2a = "0";
    String mobileFlag2b = "0";

    String flag2a = "0";
    String flag2b = "0";
    String flag2c = "0";
    String flagOthers = "0";

    String top2ndGenre = "";
    String gcf="";

    public BrowseBehaviorMetrics(String userID) {
        this.userID = userID;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }
    
    public int getIsNewUser() {
        return isNewUser;
    }

    public void setIsNewUser(int i) {
        this.isNewUser = i;
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

    public String getTransactionID() {
        return transactionID;
    }

    public void setTransactionID(String transactionID) {
        this.transactionID = transactionID;
    }

    public String getTicketID() {
        return ticketID;
    }

    public void setTicketID(String ticketID) {
        this.ticketID = ticketID;
    }

    public String getPurchaseEventDesc() {
        return purchaseEventDesc;
    }

    public void setPurchaseEventDesc(String purchaseEventDesc) {
        this.purchaseEventDesc = purchaseEventDesc;
    }

    public String getPurchaseGenreID() {
        return purchaseGenreID;
    }

    public void setPurchaseGenreID(String purchaseGenreID) {
        this.purchaseGenreID = purchaseGenreID;
    }

    public String getPurchaseGenreDescLevel1() {
        return purchaseGenreDescLevel1;
    }

    public void setPurchaseGenreDescLevel1(String purchaseGenreDescLevel1) {
        this.purchaseGenreDescLevel1 = purchaseGenreDescLevel1;
    }

    public int getUsmwebmatchEventCnt() {
        return usmwebmatchEventCnt;
    }

    public void setUsmwebmatchEventCnt(int usmwebmatchEventCnt) {
        this.usmwebmatchEventCnt = usmwebmatchEventCnt;
    }

    public int getUsmwebmatchLevel1GenreCnt() {
        return usmwebmatchLevel1GenreCnt;
    }

    public void setUsmwebmatchLevel1GenreCnt(int usmwebmatchLevel1GenreCnt) {
        this.usmwebmatchLevel1GenreCnt = usmwebmatchLevel1GenreCnt;
    }

    public int getUsmwebmatchLevel2GenreCnt() {
        return usmwebmatchLevel2GenreCnt;
    }

    public void setUsmwebmatchLevel2GenreCnt(int usmwebmatchLevel2GenreCnt) {
        this.usmwebmatchLevel2GenreCnt = usmwebmatchLevel2GenreCnt;
    }

    public int getUsmwebmatchLevel3GenreCnt() {
        return usmwebmatchLevel3GenreCnt;
    }

    public void setUsmwebmatchLevel3GenreCnt(int usmwebmatchLevel3GenreCnt) {
        this.usmwebmatchLevel3GenreCnt = usmwebmatchLevel3GenreCnt;
    }

    public int getUsmwebmatchLevel4GenreCnt() {
        return usmwebmatchLevel4GenreCnt;
    }

    public void setUsmwebmatchLevel4GenreCnt(int usmwebmatchLevel4GenreCnt) {
        this.usmwebmatchLevel4GenreCnt = usmwebmatchLevel4GenreCnt;
    }

    public int getUsmwebmatchLevel5GenreCnt() {
        return usmwebmatchLevel5GenreCnt;
    }

    public void setUsmwebmatchLevel5GenreCnt(int usmwebmatchLevel5GenreCnt) {
        this.usmwebmatchLevel5GenreCnt = usmwebmatchLevel5GenreCnt;
    }

    public int getIpadmatchEventCnt() {
        return ipadmatchEventCnt;
    }

    public void setIpadmatchEventCnt(int ipadmatchEventCnt) {
        this.ipadmatchEventCnt = ipadmatchEventCnt;
    }

    public int getIpadmatchLevel1GenreCnt() {
        return ipadmatchLevel1GenreCnt;
    }

    public void setIpadmatchLevel1GenreCnt(int ipadmatchLevel1GenreCnt) {
        this.ipadmatchLevel1GenreCnt = ipadmatchLevel1GenreCnt;
    }

    public int getIpadmatchLevel2GenreCnt() {
        return ipadmatchLevel2GenreCnt;
    }

    public void setIpadmatchLevel2GenreCnt(int ipadmatchLevel2GenreCnt) {
        this.ipadmatchLevel2GenreCnt = ipadmatchLevel2GenreCnt;
    }

    public int getIpadmatchLevel3GenreCnt() {
        return ipadmatchLevel3GenreCnt;
    }

    public void setIpadmatchLevel3GenreCnt(int ipadmatchLevel3GenreCnt) {
        this.ipadmatchLevel3GenreCnt = ipadmatchLevel3GenreCnt;
    }

    public int getIpadmatchLevel4GenreCnt() {
        return ipadmatchLevel4GenreCnt;
    }

    public void setIpadmatchLevel4GenreCnt(int ipadmatchLevel4GenreCnt) {
        this.ipadmatchLevel4GenreCnt = ipadmatchLevel4GenreCnt;
    }

    public int getIpadmatchLevel5GenreCnt() {
        return ipadmatchLevel5GenreCnt;
    }

    public void setIpadmatchLevel5GenreCnt(int ipadmatchLevel5GenreCnt) {
        this.ipadmatchLevel5GenreCnt = ipadmatchLevel5GenreCnt;
    }

    public int getMobilematchEventCnt() {
        return mobilematchEventCnt;
    }

    public void setMobilematchEventCnt(int mobilematchEventCnt) {
        this.mobilematchEventCnt = mobilematchEventCnt;
    }

    public int getMobilematchLevel1GenreCnt() {
        return mobilematchLevel1GenreCnt;
    }

    public void setMobilematchLevel1GenreCnt(int mobilematchLevel1GenreCnt) {
        this.mobilematchLevel1GenreCnt = mobilematchLevel1GenreCnt;
    }

    public int getMobilematchLevel2GenreCnt() {
        return mobilematchLevel2GenreCnt;
    }

    public void setMobilematchLevel2GenreCnt(int mobilematchLevel2GenreCnt) {
        this.mobilematchLevel2GenreCnt = mobilematchLevel2GenreCnt;
    }

    public int getMobilematchLevel3GenreCnt() {
        return mobilematchLevel3GenreCnt;
    }

    public void setMobilematchLevel3GenreCnt(int mobilematchLevel3GenreCnt) {
        this.mobilematchLevel3GenreCnt = mobilematchLevel3GenreCnt;
    }

    public int getMobilematchLevel4GenreCnt() {
        return mobilematchLevel4GenreCnt;
    }

    public void setMobilematchLevel4GenreCnt(int mobilematchLevel4GenreCnt) {
        this.mobilematchLevel4GenreCnt = mobilematchLevel4GenreCnt;
    }

    public int getMobilematchLevel5GenreCnt() {
        return mobilematchLevel5GenreCnt;
    }

    public void setMobilematchLevel5GenreCnt(int mobilematchLevel5GenreCnt) {
        this.mobilematchLevel5GenreCnt = mobilematchLevel5GenreCnt;
    }

    public String getUsmwebFlag2a() {
        return usmwebFlag2a;
    }

    public void setUsmwebFlag2a(String usmwebFlag2a) {
        this.usmwebFlag2a = usmwebFlag2a;
    }

    public String getUsmwebFlag2b() {
        return usmwebFlag2b;
    }

    public void setUsmwebFlag2b(String usmwebFlag2b) {
        this.usmwebFlag2b = usmwebFlag2b;
    }

    public String getIpadFlag2a() {
        return ipadFlag2a;
    }

    public void setIpadFlag2a(String ipadFlag2a) {
        this.ipadFlag2a = ipadFlag2a;
    }

    public String getIpadFlag2b() {
        return ipadFlag2b;
    }

    public void setIpadFlag2b(String ipadFlag2b) {
        this.ipadFlag2b = ipadFlag2b;
    }

    public String getMobileFlag2a() {
        return mobileFlag2a;
    }

    public void setMobileFlag2a(String mobileFlag2a) {
        this.mobileFlag2a = mobileFlag2a;
    }

    public String getMobileFlag2b() {
        return mobileFlag2b;
    }

    public void setMobileFlag2b(String mobileFlag2b) {
        this.mobileFlag2b = mobileFlag2b;
    }

    public String getFlag2c() {
        return flag2c;
    }

    public void setFlag2c(String flag2c) {
        this.flag2c = flag2c;
    }

    public String getFlag2a() {
        return flag2a;
    }

    public void setFlag2a(String flag2a) {
        this.flag2a = flag2a;
    }

    public String getFlag2b() {
        return flag2b;
    }

    public void setFlag2b(String flag2b) {
        this.flag2b = flag2b;
    }

    public String getFlagOthers() {
        return flagOthers;
    }

    public void setFlagOthers(String flagOthers) {
        this.flagOthers = flagOthers;
    }

    public String getTop2ndGenre() {
        return top2ndGenre;
    }

    public void setTop2ndGenre(String top2ndGenre) {
        this.top2ndGenre = top2ndGenre;
    }

    public String getGcf() {
        return gcf;
    }

    public void setGcf(String gcf) {
        this.gcf = gcf;
    }

    @Override
    public String toString() {
        return userID + ProjectConstant.DELIMITER +
                purchaseTime + ProjectConstant.DELIMITER +
                ticketID + ProjectConstant.DELIMITER +
                transactionID + ProjectConstant.DELIMITER +
                purchaseEventID + ProjectConstant.DELIMITER +
                purchaseEventDesc + ProjectConstant.DELIMITER +
                purchaseGenreID + ProjectConstant.DELIMITER +
                purchaseGenreDescLevel1 + ProjectConstant.DELIMITER +
                usmwebmatchEventCnt + ProjectConstant.DELIMITER +
                usmwebmatchLevel1GenreCnt + ProjectConstant.DELIMITER +
                usmwebmatchLevel2GenreCnt + ProjectConstant.DELIMITER +
                usmwebmatchLevel3GenreCnt + ProjectConstant.DELIMITER +
                usmwebmatchLevel4GenreCnt + ProjectConstant.DELIMITER +
                usmwebmatchLevel5GenreCnt + ProjectConstant.DELIMITER +
                usmwebFlag2a + ProjectConstant.DELIMITER +
                usmwebFlag2b + ProjectConstant.DELIMITER +
                ipadmatchEventCnt + ProjectConstant.DELIMITER +
                ipadmatchLevel1GenreCnt + ProjectConstant.DELIMITER +
                ipadmatchLevel2GenreCnt + ProjectConstant.DELIMITER +
                ipadmatchLevel3GenreCnt + ProjectConstant.DELIMITER +
                ipadmatchLevel4GenreCnt + ProjectConstant.DELIMITER +
                ipadmatchLevel5GenreCnt + ProjectConstant.DELIMITER +
                ipadFlag2a + ProjectConstant.DELIMITER +
                ipadFlag2b + ProjectConstant.DELIMITER +
                mobilematchEventCnt + ProjectConstant.DELIMITER +
                mobilematchLevel1GenreCnt + ProjectConstant.DELIMITER +
                mobilematchLevel2GenreCnt + ProjectConstant.DELIMITER +
                mobilematchLevel3GenreCnt + ProjectConstant.DELIMITER +
                mobilematchLevel4GenreCnt + ProjectConstant.DELIMITER +
                mobilematchLevel5GenreCnt + ProjectConstant.DELIMITER +
                mobileFlag2a + ProjectConstant.DELIMITER +
                mobileFlag2b + ProjectConstant.DELIMITER +
                flag2a + ProjectConstant.DELIMITER +
                flag2b + ProjectConstant.DELIMITER +
                flag2c + ProjectConstant.DELIMITER +
                flagOthers + ProjectConstant.DELIMITER +
                top2ndGenre+ ProjectConstant.DELIMITER +
                gcf+ ProjectConstant.DELIMITER +
                isNewUser
                ;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, userID);
        Text.writeString(out, purchaseTime);
        Text.writeString(out, ticketID);
        Text.writeString(out, transactionID);
        Text.writeString(out, purchaseEventID);
        Text.writeString(out, purchaseEventDesc);
        Text.writeString(out, purchaseGenreID);
        Text.writeString(out, purchaseGenreDescLevel1);
        out.writeInt(usmwebmatchEventCnt);
        out.writeInt(usmwebmatchLevel1GenreCnt);
        out.writeInt(usmwebmatchLevel2GenreCnt);
        out.writeInt(usmwebmatchLevel3GenreCnt);
        out.writeInt(usmwebmatchLevel4GenreCnt);
        out.writeInt(usmwebmatchLevel5GenreCnt);
        Text.writeString(out, usmwebFlag2a);
        Text.writeString(out, usmwebFlag2b);
        out.writeInt(ipadmatchEventCnt);
        out.writeInt(ipadmatchLevel1GenreCnt);
        out.writeInt(ipadmatchLevel2GenreCnt);
        out.writeInt(ipadmatchLevel3GenreCnt);
        out.writeInt(ipadmatchLevel4GenreCnt);
        out.writeInt(ipadmatchLevel5GenreCnt);
        Text.writeString(out, ipadFlag2a);
        Text.writeString(out, ipadFlag2b);
        out.writeInt(mobilematchEventCnt);
        out.writeInt(mobilematchLevel1GenreCnt);
        out.writeInt(mobilematchLevel2GenreCnt);
        out.writeInt(mobilematchLevel3GenreCnt);
        out.writeInt(mobilematchLevel4GenreCnt);
        out.writeInt(mobilematchLevel5GenreCnt);
        Text.writeString(out, mobileFlag2a);
        Text.writeString(out, mobileFlag2b);
        Text.writeString(out, flag2a);
        Text.writeString(out, flag2b);
        Text.writeString(out, flag2c);
        Text.writeString(out, flagOthers);
        Text.writeString(out, top2ndGenre);
        Text.writeString(out, gcf);
        out.writeInt(isNewUser);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userID = Text.readString(in);
        purchaseTime = Text.readString(in);
        ticketID = Text.readString(in);
        transactionID = Text.readString(in);
        purchaseEventID = Text.readString(in);
        purchaseEventDesc = Text.readString(in);
        purchaseGenreID = Text.readString(in);
        purchaseGenreDescLevel1 = Text.readString(in);
        usmwebmatchEventCnt = in.readInt();
        usmwebmatchLevel1GenreCnt = in.readInt();
        usmwebmatchLevel2GenreCnt = in.readInt();
        usmwebmatchLevel3GenreCnt = in.readInt();
        usmwebmatchLevel4GenreCnt = in.readInt();
        usmwebmatchLevel5GenreCnt = in.readInt();
        usmwebFlag2a = Text.readString(in);
        usmwebFlag2b = Text.readString(in);
        ipadmatchEventCnt = in.readInt();
        ipadmatchLevel1GenreCnt = in.readInt();
        ipadmatchLevel2GenreCnt = in.readInt();
        ipadmatchLevel3GenreCnt = in.readInt();
        ipadmatchLevel4GenreCnt = in.readInt();
        ipadmatchLevel5GenreCnt = in.readInt();
        ipadFlag2a = Text.readString(in);
        ipadFlag2b = Text.readString(in);
        mobilematchEventCnt = in.readInt();
        mobilematchLevel1GenreCnt = in.readInt();
        mobilematchLevel2GenreCnt = in.readInt();
        mobilematchLevel3GenreCnt = in.readInt();
        mobilematchLevel4GenreCnt = in.readInt();
        mobilematchLevel5GenreCnt = in.readInt();
        mobileFlag2a = Text.readString(in);
        mobileFlag2b = Text.readString(in);
        flag2a = Text.readString(in);
        flag2b = Text.readString(in);
        flag2c = Text.readString(in);
        flagOthers = Text.readString(in);
        top2ndGenre = Text.readString(in);
        gcf = Text.readString(in);
        isNewUser = in.readInt();
    }
}
