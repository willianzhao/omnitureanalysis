package org.willianzhao.omnitureanalysis.mapred.commons.misc.timezonehelper;

import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;

/**
 * Refer to the project : https://github.com/eBay/geosense
 * This is tailored version which only support the timezone lookup from longitude and lantitude
 * Another change is the tz_world shape file will be read from HDFS
 * Created by weilzhao on 9/28/14.
 */
public class GeoTimezone {

    private static TZWorld tzWorld;
	private static Logger logger = LoggerFactory.getLogger(GeoTimezone.class);

    public GeoTimezone(Configuration conf) {
        try {
            String resourceRoot=conf.get(ProjectConstant.PATH_RESOURCE_ROOT);
            String mapName = conf.get(ProjectConstant.PATH_TZWORLDMP_PREFIX);
                    tzWorld = new TZWorld(conf, resourceRoot, "tz_world_mp");
        } catch (Exception e) {
            //e.printStackTrace(logger.getStream(Level.ERROR));
        }
    }

    public TimeZone getTimeZone(double lat, double lon) {
        TimeZone tz = tzWorld.findTimeZone(lat, lon);
        if (tz != null) return tz;
        // fall back to a normalized Etc time zone by longitude
        int offset = (int) Math.round(lon / 15.0);
        // NOTE Etc naming convention is opposite the actual offset in hours
        tz = TimeZone.getTimeZone("Etc/GMT" + (offset <= 0 ? "+" + (-offset) : "-" + offset));
        return tz;
    }
}
