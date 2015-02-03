package org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.mapfile;

import org.willianzhao.omnitureanalysis.mapred.commons.dm.GenreIdentifier;
import org.willianzhao.omnitureanalysis.mapred.commons.hdfs.StubHubHDFSReader;
import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BloomMapFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

/**
 * Created by willianzhao on 5/15/14.
 */
@SuppressWarnings("deprecation")
public class TicketsMapFileFactory extends MapFileFactory {

    String startDate;
    String endDate;
    String upboundDate;

    DB db;
    Map<Long, Long> treeMap;

	private static Logger logger = LoggerFactory.getLogger(TicketsMapFileFactory.class);

    public TicketsMapFileFactory(Configuration conf) throws IOException {
        super(conf);
        hdfsReader = new StubHubHDFSReader(config);
        try {
            outputPath = hdfsReader.getMapFileOutputPath(ProjectConstant.TASK_MAPFILE_TICKETS);
        } catch (Exception e) {
            //do nothing
        }
        this.fs = FileSystem.get(config);
    }

    public TicketsMapFileFactory(Configuration conf, String transStartDateStr,
                                 String transEndDateStr) throws IOException, ParseException {
        this.config = conf;
        this.fs = FileSystem.get(config);
        String date_pattern = "yyyyMMdd";
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat simpleFormatter = new SimpleDateFormat(date_pattern);
        this.startDate = transStartDateStr;
        this.endDate = transEndDateStr;
        Date fileFolderEndDate = simpleFormatter.parse(transStartDateStr);
        cal.setTime(fileFolderEndDate);
        cal.add(Calendar.DATE, -ProjectConstant.MAPFILE_UPBOUND_DAYS);
        Date fileFolderBeginDate = cal.getTime();
        upboundDate = simpleFormatter.format(fileFolderBeginDate);

    }

    void initDB() {
        db = DBMaker
                .newFileDB(new File(ProjectConstant.TOKEN_MAPFILE_TICKETS))
                .mmapFileEnable()
                .closeOnJvmShutdown()
                .transactionDisable()
                .cacheSize(1000)
                .make();
        if (db.exists(ProjectConstant.TOKEN_MAPFILE_TICKETS)) {
            treeMap =
                    db.getTreeMap(ProjectConstant.TOKEN_MAPFILE_TICKETS);
        } else {
            treeMap =
                    db.createTreeMap(ProjectConstant.TOKEN_MAPFILE_TICKETS).valuesOutsideNodesEnable()
                            .nodeSize(ProjectConstant.MAPFILE_MAPDB_NODES)
                            .make();
        }
    }

    @Override
    public void loadMapFile() throws Exception {
        if (writer == null) {
            StubHubHDFSReader hdfsReader = new StubHubHDFSReader(config);
            String ticketsFileHDFSRoot = hdfsReader.getMapFileInputPathRoot(ProjectConstant.TASK_MAPFILE_TICKETS);
            if (ticketsFileHDFSRoot == null || ticketsFileHDFSRoot.trim().length() == 0) {
                throw new Exception("The tickets file hdfs path root is missing in configure file");
            }
            String outputPath = hdfsReader.getMapFileOutputPath(ProjectConstant.TASK_MAPFILE_TICKETS);
            boolean toCreateMapFile = checkIfNeedMapFileToCreate();
            if (toCreateMapFile) {
                boolean constructStatus = constructMapFile(ticketsFileHDFSRoot);
                if (!constructStatus) {
                    throw new Exception("Failed to construct the map file object for tickets files");

                } else {
                    logger.info("Successfully create the map file for tickets files");
                }
            } else {
                logger.info("We found an existing tickets mapfile {} hence reuse its data.", outputPath);
            }
        }

    }

    boolean constructMapFile(String pathRoot) {
        initDB();
        try {
            long startTime = System.currentTimeMillis();
            FileStatus[] statusList = fs.listStatus(new Path(pathRoot));
            for (FileStatus status : statusList) {
                Path path = status.getPath();

                /*
                Considering the data volume , limit the tickets files starting from startDate and end on 365 days ago
                 */
                String currentPath = path.toString();
                String currentPathDate = currentPath.substring(currentPath.lastIndexOf("/") + 1);
                if (currentPathDate.compareTo(upboundDate) < 0 || currentPathDate.compareTo(endDate) > 0) {
                    continue;
                }
                if (fs.isDirectory(path)) {
                    FileStatus[] innerStatusList = fs.listStatus(path);
                    if (innerStatusList.length > 0) {
                        path = innerStatusList[0].getPath();
                    } else {
                        continue;
                    }
                }
                logger.debug("Processing the file {}", path);
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                constructSortedCollection(br);
                logger.info("Finish processing the file {}", path);
            }
            long costTime = (System.currentTimeMillis() - startTime) / 1000;
            logger.info("Reading the HDFS files and then construct memory treemap object totally cost {} seconds",
                    costTime);
            logger.info("Start to create the mapfile for tickets");
            LongWritable key = new LongWritable();
            LongWritable value = new LongWritable();
            writer = new BloomMapFile.Writer(config, fs, outputPath, key.getClass(), value.getClass());
            writer.setIndexInterval(4);
            startTime = System.currentTimeMillis();
            for (Map.Entry<Long, Long> entry : treeMap.entrySet()) {
                Long ticketID = entry.getKey();
                Long eventID = entry.getValue();
                key.set(ticketID);
                value.set(eventID);
                writer.append(key, value);
            }
            costTime = (System.currentTimeMillis() - startTime) / 1000;
            logger.info("Successfully created the mapfile.Totally cost {} seconds", costTime);
            markTimestamp(outputPath);
            return true;
        } catch (IOException e) {
            logger.error("Failed to process the HDFS files under {}", pathRoot);
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
            String mapFileOutputPath = hdfsReader.getMapFileOutputPath(ProjectConstant.TASK_MAPFILE_TICKETS);
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
        String line;
        line = br.readLine();
        while (line != null) {
            String[] fields = line.split(ProjectConstant.CONTROLA_REC_DELIMITER);
            //Get the 1st and 2nd for ticketID and eventID
            Long ticketID = Long.parseLong(fields[ProjectConstant.CONSTANT_TICKETS_TICKETID_POS]);
            Long eventID = Long.parseLong(fields[ProjectConstant.CONSTANT_TICKETS_EVENTID_POS]);
            treeMap.put(ticketID, eventID);
            line = br.readLine();
        }

    }
}
