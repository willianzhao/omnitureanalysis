package org.willianzhao.omnitureanalysis.mapred.commons.model;

import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by weilzhao on 8/21/14.
 */
public class TransactionGeography implements WritableComparable {

    String transactionID = "";
    String purchaseTime = "";
    String IPLantitude = "";
    String IPLongitude = "";
    String IPCity = "";
    String IPState = "";
    String IPCountry = "";
    String IPZipcode = "";
    String IPTimezone = "";
    String IPGeoSource = "";
    String zipcodeLantitude = "";
    String zipcodeLongitude = "";
    String zipcodeCity = "";
    String zipcodeState = "";
    String zipcodeCountry = "";
    String zipcode = "";
    String zipcodeTimezone = "";
    String zipcodeGeoSource = "";
    String localTime="";
    String localDate="";
    String localHourOfDay="";

    public TransactionGeography(){

    }

    public TransactionGeography(String transactionID, String purchaseTime) {
        this.transactionID = transactionID;
        this.purchaseTime = purchaseTime;
    }

    public String getTransactionID() {
        return transactionID;
    }

    public String getPurchaseTime() {
        return purchaseTime;
    }

    public String getIPLantitude() {
        return IPLantitude;
    }

    public void setIPLantitude(String IPLantitude) {
        this.IPLantitude = IPLantitude;
    }

    public String getIPLongitude() {
        return IPLongitude;
    }

    public void setIPLongitude(String IPLongitude) {
        this.IPLongitude = IPLongitude;
    }

    public String getIPCity() {
        return IPCity;
    }

    public void setIPCity(String IPCity) {
        this.IPCity = IPCity;
    }

    public String getIPState() {
        return IPState;
    }

    public void setIPState(String IPState) {
        this.IPState = IPState;
    }

    public String getIPCountry() {
        return IPCountry;
    }

    public void setIPCountry(String IPCountry) {
        this.IPCountry = IPCountry;
    }

    public String getIPTimezone() {
        return IPTimezone;
    }

    public void setIPTimezone(String IPTimezone) {
        this.IPTimezone = IPTimezone;
    }

    public String getIPGeoSource() {
        return IPGeoSource;
    }

    public void setIPGeoSource(String IPGeoSource) {
        this.IPGeoSource = IPGeoSource;
    }

    public String getZipcodeLantitude() {
        return zipcodeLantitude;
    }

    public void setZipcodeLantitude(String zipcodeLantitude) {
        this.zipcodeLantitude = zipcodeLantitude;
    }

    public String getZipcodeLongitude() {
        return zipcodeLongitude;
    }

    public void setZipcodeLongitude(String zipcodeLongitude) {
        this.zipcodeLongitude = zipcodeLongitude;
    }

    public String getZipcodeCity() {
        return zipcodeCity;
    }

    public void setZipcodeCity(String zipcodeCity) {
        this.zipcodeCity = zipcodeCity;
    }

    public String getZipcodeState() {
        return zipcodeState;
    }

    public void setZipcodeState(String zipcodeState) {
        this.zipcodeState = zipcodeState;
    }

    public String getZipcodeCountry() {
        return zipcodeCountry;
    }

    public void setZipcodeCountry(String zipcodeCountry) {
        this.zipcodeCountry = zipcodeCountry;
    }

    public String getZipcodeTimezone() {
        return zipcodeTimezone;
    }

    public void setZipcodeTimezone(String zipcodeTimezone) {
        this.zipcodeTimezone = zipcodeTimezone;
    }

    public String getZipcodeGeoSource() {
        return zipcodeGeoSource;
    }

    public void setZipcodeGeoSource(String zipcodeGeoSource) {
        this.zipcodeGeoSource = zipcodeGeoSource;
    }

    public String getLocalTime() {
        return localTime;
    }

    public void setLocalTime(String localTime) {
        this.localTime = localTime;
    }

    public String getLocalDate() {
        return localDate;
    }

    public void setLocalDate(String localDate) {
        this.localDate = localDate;
    }

    public String getLocalHourOfDay() {
        return localHourOfDay;
    }

    public void setLocalHourOfDay(String localHourOfDay) {
        this.localHourOfDay = localHourOfDay;
    }

    public String getIPZipcode() {
        return IPZipcode;
    }

    public void setIPZipcode(String IPZipcode) {
        this.IPZipcode = IPZipcode;
    }

    public String getZipcode() {
        return zipcode;
    }

    public void setZipcode(String zipcode) {
        this.zipcode = zipcode;
    }

    @Override
    public String toString() {
        return transactionID + ProjectConstant.DELIMITER +
                purchaseTime + ProjectConstant.DELIMITER +
                IPLantitude + ProjectConstant.DELIMITER +
                IPLongitude + ProjectConstant.DELIMITER +
                IPCity + ProjectConstant.DELIMITER +
                IPState + ProjectConstant.DELIMITER +
                IPCountry + ProjectConstant.DELIMITER +
                IPZipcode + ProjectConstant.DELIMITER +
                IPTimezone + ProjectConstant.DELIMITER +
                IPGeoSource + ProjectConstant.DELIMITER +
                zipcodeLantitude + ProjectConstant.DELIMITER +
                zipcodeLongitude + ProjectConstant.DELIMITER +
                zipcodeCity + ProjectConstant.DELIMITER +
                zipcodeState + ProjectConstant.DELIMITER +
                zipcodeCountry + ProjectConstant.DELIMITER +
                zipcode + ProjectConstant.DELIMITER +
                zipcodeTimezone + ProjectConstant.DELIMITER +
                zipcodeGeoSource + ProjectConstant.DELIMITER +
                localTime + ProjectConstant.DELIMITER +
                localDate + ProjectConstant.DELIMITER +
                localHourOfDay;
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof TransactionGeography) {
            TransactionGeography other = (TransactionGeography) o;
            return this.toString().compareTo(other.toString());
        } else {
            return 1;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, transactionID);
        Text.writeString(out, purchaseTime);
        Text.writeString(out, IPLantitude);
        Text.writeString(out, IPLongitude);
        Text.writeString(out, IPCity);
        Text.writeString(out, IPState);
        Text.writeString(out, IPCountry);
        Text.writeString(out, IPTimezone);
        Text.writeString(out, IPGeoSource);
        Text.writeString(out, zipcodeLantitude);
        Text.writeString(out, zipcodeLongitude);
        Text.writeString(out, zipcodeCity);
        Text.writeString(out, zipcodeState);
        Text.writeString(out, zipcodeCountry);
        Text.writeString(out, zipcodeTimezone);
        Text.writeString(out, localTime);
        Text.writeString(out, localDate);
        Text.writeString(out, localHourOfDay);
        Text.writeString(out, IPZipcode);
        Text.writeString(out, zipcode);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        transactionID = Text.readString(in);
        purchaseTime = Text.readString(in);
        IPLantitude = Text.readString(in);
        IPLongitude = Text.readString(in);
        IPCity = Text.readString(in);
        IPState = Text.readString(in);
        IPCountry = Text.readString(in);
        IPTimezone = Text.readString(in);
        IPGeoSource = Text.readString(in);
        zipcodeLantitude = Text.readString(in);
        zipcodeLongitude = Text.readString(in);
        zipcodeCity = Text.readString(in);
        zipcodeState = Text.readString(in);
        zipcodeCountry = Text.readString(in);
        zipcodeTimezone = Text.readString(in);
        localTime = Text.readString(in);
        localDate = Text.readString(in);
        localHourOfDay = Text.readString(in);
        IPZipcode = Text.readString(in);
        zipcode = Text.readString(in);
    }
}
