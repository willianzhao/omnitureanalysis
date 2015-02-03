package org.willianzhao.omnitureanalysis.mapred.commons.model;

import org.willianzhao.omnitureanalysis.mapred.commons.dm.GenreIdentifier;
import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

/**
 * Created by willianzhao on 5/7/14.
 */
public class GenreFactory {
    /*
    The genre leaf map is to store <GenreID, GenerLevel>.
    GenreLevel is the map of <Level, GenreNode>
     */
    private Configuration conf;
    private Path genreFile;
    private HashMap<String, HashMap<Integer, GenreNode>> genreLeafMap;
	private static Logger logger = LoggerFactory.getLogger(GenreIdentifier.class);

    
    public GenreFactory(Configuration config,
                        Path genreFilePath) {
        conf = config;
        genreFile = genreFilePath;
    }

    public HashMap<String, HashMap<Integer, GenreNode>> getGenreLeafMap() throws Exception {
        if (genreLeafMap == null || genreLeafMap.size() == 0) {
            genreLeafMap = new HashMap<String, HashMap<Integer, GenreNode>>();
            constructMap(conf, genreFile);

        }
        return genreLeafMap;
    }

    void constructMap(Configuration conf, Path genreFile) throws Exception {
        HashMap<String, GenreNode> genreNodesList = readFromGenreFile(conf, genreFile);
        if (genreNodesList.size() > 0) {
            recursiveCreateGenreHierarchy(genreNodesList);
        }
    }

    HashMap<String, GenreNode> readFromGenreFile(Configuration conf, Path genreFile) throws IOException {
        HashMap<String, GenreNode> genreList = new HashMap<String, GenreNode>();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(genreFile)) {
            FSDataInputStream in = fs.open(genreFile);
            BufferedReader br = new BufferedReader(new InputStreamReader(new DataInputStream(in)));
            String line;
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(ProjectConstant.CONTROLA_REC_DELIMITER);
                String genreID = fields[0];
                String parentID = fields[1];
                String genreDesc = fields[2];
                String gcfID;
                if (fields.length == ProjectConstant.GENRE_CUT_FINAL_FIELD_CNT) {
                    gcfID = fields[20].trim();
                } else {
                    gcfID = "";
                }
                GenreNode node = new GenreNode(genreID, parentID, genreDesc, gcfID);
                genreList.put(genreID, node);
            }
            br.close();
            in.close();
        } else {
            throw new IOException("The genre file is not existed in HDFS path");
        }
        return genreList;
    }

    void recursiveCreateGenreHierarchy(HashMap<String, GenreNode> rawGenreMap) {
        //TODO: in future it should support only leaf node and remove the non-leaf node from map
        for (String genreID : rawGenreMap.keySet()) {
            if (!genreID.equals("0")) {
                HashMap<Integer, GenreNode> genreLevelMap = new HashMap<Integer, GenreNode>();
                int level = 1;
                recursiveLookup(genreID, rawGenreMap, genreLevelMap, level);
                int treeLevels = genreLevelMap.size();
                if (treeLevels > 0) {
                    genreLeafMap.put(genreID, genreLevelMap);

                } else {
                    logger.error("Failed to construct genre tree for genre {}", genreID);
                }
            }
        }

    }

    void recursiveLookup(String genreID, HashMap<String, GenreNode> rawGenreMap,
                         HashMap<Integer, GenreNode> genreLevelMap, int level) {
        GenreNode gn = rawGenreMap.get(genreID);
        if (gn != null) {
            String parentID = gn.getParentID();
            if (!parentID.equals("-1")) {
                try {
                    genreLevelMap.put(level, ReflectionUtils.copy(conf, gn, new GenreNode()));
                    recursiveLookup(parentID, rawGenreMap, genreLevelMap, level + 1);

                } catch (IOException e) {
                   // e.printStackTrace(logger.getStream(Level.ERROR));
                    logger.error("Error and exit for genre node {} processing", genreID);
                }

            }
        }

    }

}
