package org.willianzhao.omnitureanalysis.mapred.commons.dm;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by willianzhao on 3/25/14.
 */
public class TimeIdentifier extends SmartIdentifier {


    public TimeIdentifier(Configuration config, String[] inputFields) {
        super(config, inputFields);
    }

    public String getHitTime() {
        int position = 0;
        try {
            // hit_time_gmt
            position = Integer.parseInt(conf.get("HIT_TIME_GMT"));
            String hit_time_gmt = rawFields[position].trim();
            if (hit_time_gmt.length() > 0) {
                //TODO: here it needs to convert to date format
                return hit_time_gmt;
            } else {
                return "";
            }
        } catch (Exception aie) {
            return "";
        }
    }

    public String getVisitTime() {
        int position = 0;
        try {
            // date_time
            position = Integer.parseInt(conf.get("DATE_TIME"));
            String visit_time = rawFields[position].trim();
            if (visit_time.length() > 0) {
                return visit_time;
            } else {
                return "";
            }
        } catch (Exception aie) {
            return "";
        }
    }
}
