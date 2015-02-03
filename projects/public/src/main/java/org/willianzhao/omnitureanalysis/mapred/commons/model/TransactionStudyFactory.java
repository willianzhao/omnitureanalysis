package org.willianzhao.omnitureanalysis.mapred.commons.model;

import org.willianzhao.omnitureanalysis.mapred.commons.dm.LocationIdentifier;
import org.willianzhao.omnitureanalysis.mapred.commons.dm.PageIdentifier;
import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Created by weilzhao on 8/25/14.
 */
public class TransactionStudyFactory extends MRBaseFactory {

    public TransactionStudyFactory(Configuration conf, Path inputSplitPath) throws Exception {
        super(conf, inputSplitPath);
    }

    public TransactionRecord readFromStubTrans(String[] rawFields) {
        TransactionRecord tRecord = new TransactionRecord();
        if (rawFields.length == 10) {
            //Use buyerID not the guid as the user ID in transaction record
            tRecord.setUserID(rawFields[8]);
            tRecord.setPurchaseTime(rawFields[1]);
            tRecord.setTransactionID(rawFields[2]);
            tRecord.setTicketID(rawFields[3]);
            tRecord.setPurchaseEventID(rawFields[4]);
            tRecord.setPurchaseGenreID(rawFields[5]);
            tRecord.setDataSource(ProjectConstant.SOURCE_STUB_TRANS);
        }
        return tRecord;
    }

    public TransactionRecord readFromOmniture(String[] rawFields) {
        String transactionID;
        String ip;
        String geoCity;
        String geoRegion;
        String geoCountry;
        String geoZipcode;
        //Construct the identifier instances
        TransactionRecord tRecord = new TransactionRecord();
        PageIdentifier pageIden = new PageIdentifier(conf, rawFields);
        LocationIdentifier locIden = new LocationIdentifier(conf, rawFields);
        transactionID = pageIden.getTransactionID();
        ip = locIden.getIPAddress();
        geoCity = locIden.getGeoCity();
        geoRegion = locIden.getGeoRegion();
        geoCountry = locIden.getGeoCountry();
        geoZipcode = locIden.getGeoZipcode();
        tRecord.setTransactionID(transactionID);
        tRecord.setIp(ip);
        tRecord.setGeoCity(geoCity);
        tRecord.setGeoRegion(geoRegion);
        tRecord.setGeoCountry(geoCountry);
        tRecord.setGeoZipcode(geoZipcode);
        tRecord.setDataSource(ProjectConstant.SOURCE_OMNITURE);
        return tRecord;
    }
}
