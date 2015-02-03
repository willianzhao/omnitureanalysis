package org.willianzhao.omnitureanalysis.mapred.commons.dm;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by willianzhao on 3/24/14.
 */
public class LocationIdentifier extends SmartIdentifier {

	private static Logger logger = LoggerFactory.getLogger(LocationIdentifier.class);

    public LocationIdentifier(Configuration config, String[] inputFields) {
        super(config, inputFields);
    }

    public String getIPAddress() {
        int position = 0;
        try {
            // ip , ip2
            position = Integer.parseInt(conf.get("IP"));
            String ip = rawFields[position].trim();
            if (ip.length() == 0) {
                position = Integer.parseInt(conf.get("IP2"));
                ip = rawFields[position].trim();

            }
            return ip;
        } catch (Exception aie) {
            return "";
        }
    }

    public String getGeoCity() {
        int position = 0;
        try {
            position = Integer.parseInt(conf.get("GEO_CITY"));
            String geoCity = rawFields[position].trim();
            if (geoCity.length() > 0) {
                return geoCity;
            } else {
                return "";
            }
        } catch (Exception aie) {
            return "";
        }

    }

    public String getGeoRegion() {
        int position = 0;
        try {
            position = Integer.parseInt(conf.get("GEO_REGION"));
            String geoRegion = rawFields[position].trim();
            if (geoRegion.length() > 0) {
                return geoRegion;
            } else {
                return "";
            }
        } catch (Exception aie) {
            return "";
        }

    }

    public String getGeoCountry() {
        int position = 0;
        try {
            position = Integer.parseInt(conf.get("GEO_COUNTRY"));
            String geoCountry = rawFields[position].trim();
            if (geoCountry.length() > 0) {
                return geoCountry;
            } else {
                return "";
            }
        } catch (Exception aie) {
            return "";
        }

    }

    public String getGeoZipcode() {
        int position = 0;
        try {
            position = Integer.parseInt(conf.get("GEO_ZIP"));
            String geoZipcode = rawFields[position].trim();
            if (geoZipcode.length() > 0) {
                return geoZipcode;
            } else {
                return "";
            }
        } catch (Exception aie) {
            return "";
        }

    }
}
