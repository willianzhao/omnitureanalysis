package org.willianzhao.omnitureanalysis.mapred.commons.model;

import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by weilzhao on 8/18/14.
 */
public class TransactionRecord implements WritableComparable {

    String userID = "";
    String purchaseTime = "";
    String ticketID = "";
    String transactionID = "";
    String purchaseEventID = "";
    String purchaseEventDesc = "";
    String purchaseGenreID = "";
    String purchaseGenreDescLevel1 = "";
    String gcf = "";
    String geoCity = "";
    String geoRegion = "";
    String geoCountry = "";
    String geoZipcode = "";
    String ip = "";
    String regCity = "";
    String regRegion = "";
    String regCountry = "";
    String regZipcode = "";
    String dataSource = "";

    public TransactionRecord() {
    }

    public TransactionRecord(String transactionID, String dataSource) {
        this.transactionID = transactionID;
        this.dataSource = dataSource;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public String getPurchaseTime() {
        return purchaseTime;
    }

    public void setPurchaseTime(String purchaseTime) {
        this.purchaseTime = purchaseTime;
    }

    public String getTicketID() {
        return ticketID;
    }

    public void setTicketID(String ticketID) {
        this.ticketID = ticketID;
    }

    public String getTransactionID() {
        return transactionID;
    }

    public void setTransactionID(String transactionID) {
        this.transactionID = transactionID;
    }

    public String getPurchaseEventID() {
        return purchaseEventID;
    }

    public void setPurchaseEventID(String purchaseEventID) {
        this.purchaseEventID = purchaseEventID;
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

    public String getGcf() {
        return gcf;
    }

    public void setGcf(String gcf) {
        this.gcf = gcf;
    }

    public String getGeoCity() {
        return geoCity;
    }

    public void setGeoCity(String geoCity) {
        this.geoCity = geoCity;
    }

    public String getGeoCountry() {
        return geoCountry;
    }

    public void setGeoCountry(String geoCountry) {
        this.geoCountry = geoCountry;
    }

    public String getGeoZipcode() {
        return geoZipcode;
    }

    public void setGeoZipcode(String geoZipcode) {
        this.geoZipcode = geoZipcode;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getRegCity() {
        return regCity;
    }

    public void setRegCity(String regCity) {
        this.regCity = regCity;
    }

    public String getRegRegion() {
        return regRegion;
    }

    public void setRegRegion(String regRegion) {
        this.regRegion = regRegion;
    }

    public String getRegZipcode() {
        return regZipcode;
    }

    public void setRegZipcode(String regZipcode) {
        this.regZipcode = regZipcode;
    }

    public String getGeoRegion() {
        return geoRegion;
    }

    public void setGeoRegion(String geoRegion) {
        this.geoRegion = geoRegion;
    }

    public String getRegCountry() {
        return regCountry;
    }

    public void setRegCountry(String regCountry) {
        this.regCountry = regCountry;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
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
                gcf + ProjectConstant.DELIMITER +
                geoCity + ProjectConstant.DELIMITER +
                geoRegion + ProjectConstant.DELIMITER +
                geoCountry + ProjectConstant.DELIMITER +
                geoZipcode + ProjectConstant.DELIMITER +
                ip + ProjectConstant.DELIMITER +
                regCity + ProjectConstant.DELIMITER +
                regRegion + ProjectConstant.DELIMITER +
                regCountry + ProjectConstant.DELIMITER +
                regZipcode;
    }

    public String getBusinessKey() {
        return userID + ProjectConstant.DELIMITER +
                transactionID;
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof TransactionRecord) {
            TransactionRecord other = (TransactionRecord) o;
            return this.getBusinessKey().compareTo(other.getBusinessKey());
        } else {
            return 1;
        }
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
        Text.writeString(out, gcf);
        Text.writeString(out, geoCity);
        Text.writeString(out, geoRegion);
        Text.writeString(out, geoCountry);
        Text.writeString(out, geoZipcode);
        Text.writeString(out, ip);
        Text.writeString(out, regCity);
        Text.writeString(out, regRegion);
        Text.writeString(out, regCountry);
        Text.writeString(out, regZipcode);
        Text.writeString(out, dataSource);
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
        gcf = Text.readString(in);
        geoCity = Text.readString(in);
        geoRegion = Text.readString(in);
        geoCountry = Text.readString(in);
        geoZipcode = Text.readString(in);
        ip = Text.readString(in);
        regCity = Text.readString(in);
        regRegion = Text.readString(in);
        regCountry = Text.readString(in);
        regZipcode = Text.readString(in);
        dataSource = Text.readString(in);
    }
}
