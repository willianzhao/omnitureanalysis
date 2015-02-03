package org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.jobs;

import org.apache.hadoop.conf.Configuration;

import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.mapfile.EventsMapFileFactory;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.mapfile.GeonamesZipcodeMapFileFactory;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.mapfile.TicketsMapFileFactory;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.mapfile.UserContactsMapFileFactory;
import org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.mapfile.UserMapFileFactory;

/**
 * Created by willianzhao on 6/8/14.
 */
public class MapFileClient {

    Configuration config;

    public MapFileClient(Configuration config) {
        this.config = config;
    }

    public void createEventMapFile(String endDateStr) throws Exception {
        EventsMapFileFactory eventMapFile = null;
        eventMapFile = new EventsMapFileFactory(config);
        eventMapFile.loadMapFile();
    }

    public void createTicketMapFile(String startDateStr, String endDateStr) throws Exception {
        TicketsMapFileFactory ticketMapFile = new TicketsMapFileFactory(config, startDateStr, endDateStr);
        ticketMapFile.loadMapFile();
    }

    public void createUserMapFIle(String endDateStr) throws Exception {
        UserMapFileFactory userMapFile = new UserMapFileFactory(config);
        userMapFile.loadMapFile();
    }

    public void createUserContactsMapFIle(String endDateStr) throws Exception {
        UserContactsMapFileFactory userContactsMapFile = new UserContactsMapFileFactory(config);
        userContactsMapFile.loadMapFile();
    }

    public void createZipcodeLookupMapFIle() throws Exception {
        GeonamesZipcodeMapFileFactory geonamesMapfile = new GeonamesZipcodeMapFileFactory(config);
        geonamesMapfile.loadMapFile();
    }
}
