package org.willianzhao.omnitureanalysis.mapred.commons.misc;

import org.willianzhao.omnitureanalysis.mapred.commons.model.BrowseHistory;

import java.util.Comparator;

/**
 * Created by willianzhao on 5/9/14.
 */
public class BrowseHistoryComparator implements Comparator<BrowseHistory> {

    @Override
    public int compare(BrowseHistory browseHistory, BrowseHistory browseHistory2) {
        //Descendant sort the result then the latest visit record could be put to front
        int result = browseHistory.compareTo(browseHistory2);
        if (result < 0) {
            return 1;
        } else if (result > 0) {
            return -1;
        } else {
            return 0;
        }
    }
}
