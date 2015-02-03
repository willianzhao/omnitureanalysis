package org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.mapfile;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BloomMapFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.willianzhao.omnitureanalysis.mapred.commons.hdfs.StubHubHDFSReader;
import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;

/**
 * Created by willianzhao on 5/15/14.
 */

public class EventsMapFileFactory extends MapFileFactory {

    DB db;

    /*
    The key is eventID, the value is a composite data structure : [event description] ~ [genre ID]
     */
    Map<Long, String> treeMap;

	private static Logger logger = LoggerFactory.getLogger(EventsMapFileFactory.class);

    public EventsMapFileFactory(Configuration conf) throws IOException {
        super(conf);
        hdfsReader = new StubHubHDFSReader(config);
        try {
            outputPath = hdfsReader.getMapFileOutputPath(ProjectConstant.TASK_MAPFILE_EVENTS);
        } catch (Exception e) {
            //do nothing
        }
        this.fs = FileSystem.get(config);
    }

    void initDB() {
        db = DBMaker
                .newFileDB(new File(ProjectConstant.TOKEN_MAPFILE_EVENTS))
                .mmapFileEnable()
                .closeOnJvmShutdown()
                .transactionDisable()
                .cacheSize(1000)
                .make();
        if (db.exists(ProjectConstant.TOKEN_MAPFILE_EVENTS)) {
            treeMap =
                    db.getTreeMap(ProjectConstant.TOKEN_MAPFILE_EVENTS);
        } else {
            treeMap =
                    db.createTreeMap(ProjectConstant.TOKEN_MAPFILE_EVENTS).valuesOutsideNodesEnable()
                            .nodeSize(ProjectConstant.MAPFILE_MAPDB_NODES)
                            .make();
        }
    }

    @Override
    public void loadMapFile() throws Exception {
        if (writer == null) {
            StubHubHDFSReader hdfsReader = new StubHubHDFSReader(config);
            String eventsFileHDFSRoot = hdfsReader.getMapFileInputPathRoot(ProjectConstant.TASK_MAPFILE_EVENTS);
            if (eventsFileHDFSRoot == null || eventsFileHDFSRoot.trim().length() == 0) {
                throw new Exception("The event file hdfs path root is missing in configure file");
            }
            String outputPath = hdfsReader.getMapFileOutputPath(ProjectConstant.TASK_MAPFILE_EVENTS);
            boolean toCreateMapFile = checkIfNeedMapFileToCreate();
            if (toCreateMapFile) {
                boolean constructStatus = constructMapFile(eventsFileHDFSRoot);
                if (!constructStatus) {
                    throw new Exception("Failed to construct the map file object for events files");

                } else {
                    logger.info("Successfully create the map file for events files");
                }
            } else {
                logger.info("We found an existing events mapfile {} hence reuse its data.", outputPath);
            }
        }

    }

    boolean constructMapFile(String inputPathRoot) {
        boolean requireProcessed = false;
        long startTime = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder();
        initDB();
        try {
            /*
            Performance tips:
            In addition to avoiding the text parsing overhead, the binary Writable types will take up less space as intermediate data.Although Text can be convenient, converting numeric data to and from UTF8 strings is inefficient and can actually make up a significant portion of CPU time. Whenever dealing with non-textual data, consider using the binary Writables like IntWritable.
             */
            FileStatus[] statusList = fs.listStatus(new Path(inputPathRoot));
            for (FileStatus status : statusList) {
                Path path = status.getPath();
                if (status.isDir()) {
                    FileStatus[] innerStatusList = fs.listStatus(path);
                    if (innerStatusList.length > 0) {
                        path = innerStatusList[0].getPath();
                    } else {
                        logger.info("Skip this directory because there is no file existed");
                        continue;
                    }
                    try {
                        requireProcessed = isForIncrementalLoading(path);
                    } catch (Exception e) {
                        logger.error("Exception happend when processing path {}", path);
                        //e.printStackTrace(logger.getStream(Level.ERROR));
                        requireProcessed = false;
                    }
                    if (requireProcessed) {
                        logger.debug("Processing the file {}", path);
                        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                        constructSortedCollection(br);
                        logger.info("Finish load the file {}", path);
                        sb.append(path.getName()).append(",");
                    } else {
                        logger.info("Exclude path {} from the valid scope.", path);
                    }
                    recordMaxProcessedCal(path);
                } else {
                    logger.error("The path {} is not directory but file. Skip it.", path);
                }
            }
            long costTime = (System.currentTimeMillis() - startTime) / 1000;
            logger.info("Loading the HDFS files {} \nConstruct memory treemap object totally cost {} seconds",
                    sb.toString(), costTime);
            logger.info("Start to create the mapfile for events");
            LongWritable key = new LongWritable();
            Text value = new Text();
            writer = new BloomMapFile.Writer(config, fs, outputPath, key.getClass(), value.getClass());
            writer.setIndexInterval(20);
            startTime = System.currentTimeMillis();
            for (Map.Entry<Long, String> entry : treeMap.entrySet()) {
                Long eventID = entry.getKey();
                String valueStr = entry.getValue();
                key.set(eventID);
                value.set(valueStr);
                writer.append(key, value);
            }
            costTime = (System.currentTimeMillis() - startTime) / 1000;
            logger.info("Successfully created the mapfile. Totally cost {} seconds", costTime);
            markTimestamp(outputPath);
            return true;
        } catch (IOException e) {
            logger.error("Failed to process the HDFS files under {}", inputPathRoot);
            //e.printStackTrace(logger.getStream(Level.ERROR));
        } catch (ParseException e) {
            //e.printStackTrace(logger.getStream(Level.ERROR));
        } finally {
            IOUtils.closeStream(writer);
            db.close();
        }
        return false;
    }

    @Override
    public MapFile.Reader getMapfileReader() throws Exception {
        if (lookupReader == null) {
            StubHubHDFSReader hdfsReader = new StubHubHDFSReader(config);
            String mapFileOutputPath = hdfsReader.getMapFileOutputPath(ProjectConstant.TASK_MAPFILE_EVENTS);
            lookupReader = new MapFile.Reader(fs, mapFileOutputPath, config);
        }
        return lookupReader;
    }

    void constructSortedCollection(BufferedReader br) throws IOException {

        /*
        Performance tips:

        Because we need to load huge size of elements into collection and sort on it, so memory consumption and time cost are factors to chose appropriate Java data structure.
        There are two options.

        A. create primitive collections first then sort
        B. use sorted data structure like TreeMap (red-black tree)

        We need to consider following factors:

        1. The cost of inserting element

        Adding a single item to an ArrayList containing a million items could take a very long time -- it's an O(n) operation (add(E element) is O(1) amortized, but O(n) worst-case since the array must be resized and copied) plus double the storage unless you preallocated space. Adding an item to a LinkedList is O(1).
        LinkedList is most efficient when remove one element from Iterator (O(1))

        The implementation of TreeMap provides guaranteed log(n) time cost for the containsKey, get, put and remove operations.

        2. Memory consumption

        Assuming that a reference occupies 4 bytes:

        ArrayList : 4 bytes (but may be more if ArrayList capacity is seriously more than its size
        LinkedList : 24 bytes (fixed)
        TreeMap, TreeSet : 40 * SIZE bytes

        3. Sort operation

        Java uses quicksort for primitive objects as it is faster than merge sort in the average case . It uses merge sort for sorting objects as merge sort is a stable sorting algorithm.

        TreeMap is already sorted

        More investigation :

        By comparing to in-memory key-value store products, MapDB has been chose to replace the Java TreeMap to store and sort kv paris.
         */
        int foundExsitingCnt = 0;
        String line;
        line = br.readLine();
        while (line != null) {
            String[] fields = line.split(ProjectConstant.CONTROLA_REC_DELIMITER);
            Long eventID = Long.parseLong(fields[ProjectConstant.CONSTANT_EVENTS_EVENTID_POS]);
            //Skip the existing keys
            if (treeMap.get(eventID) == null) {
                String genreID = fields[ProjectConstant.CONSTANT_EVENTS_GENREID_POS];
                String eventDesc = fields[ProjectConstant.CONSTANT_EVENTS_DESC_POS];
                String value = genreID + ProjectConstant.DELIMITER + eventDesc;
                treeMap.put(eventID, value);
            } else {
                foundExsitingCnt++;
            }
            line = br.readLine();
        }
        logger.debug("Found existing {} event records in mapdb database file", foundExsitingCnt);
    }
}
