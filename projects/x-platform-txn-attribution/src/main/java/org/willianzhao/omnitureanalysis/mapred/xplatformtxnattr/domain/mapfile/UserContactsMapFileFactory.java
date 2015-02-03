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
 * Created by weilzhao on 8/19/14.
 */
public class UserContactsMapFileFactory extends MapFileFactory {

    DB db;
    Map<Long, String> treeMap;
	private static Logger logger = LoggerFactory.getLogger(UserContactsMapFileFactory.class);

    public UserContactsMapFileFactory(Configuration conf) throws IOException {
        super(conf);
        hdfsReader = new StubHubHDFSReader(config);
        try {
            outputPath = hdfsReader.getMapFileOutputPath(ProjectConstant.TASK_MAPFILE_USER_CONTACTS);
        } catch (Exception e) {
            //do nothing
        }
        this.fs = FileSystem.get(config);
    }

    void initDB() {
        db = DBMaker
                .newFileDB(new File(ProjectConstant.TOKEN_MAPFILE_USER_CONTACTS))
                .mmapFileEnable()
                .closeOnJvmShutdown()
                .transactionDisable()
                .cacheSize(1000)
                .make();
        if (db.exists(ProjectConstant.TOKEN_MAPFILE_USER_CONTACTS)) {
            treeMap =
                    db.getTreeMap(ProjectConstant.TOKEN_MAPFILE_USER_CONTACTS);
        } else {
            treeMap =
                    db.createTreeMap(ProjectConstant.TOKEN_MAPFILE_USER_CONTACTS).valuesOutsideNodesEnable()
                            .nodeSize(ProjectConstant.MAPFILE_MAPDB_NODES)
                            .make();
        }
    }

    @Override
    public void loadMapFile() throws Exception {
        if (writer == null) {
            StubHubHDFSReader hdfsReader = new StubHubHDFSReader(config);
            String userContactsFileHDFSRoot = hdfsReader.getMapFileInputPathRoot(ProjectConstant.TASK_MAPFILE_USER_CONTACTS);
            if (userContactsFileHDFSRoot == null || userContactsFileHDFSRoot.trim().length() == 0) {
                throw new Exception("The user_contacts file hdfs path root is missing in configure file");
            }
            String outputPath = hdfsReader.getMapFileOutputPath(ProjectConstant.TASK_MAPFILE_USER_CONTACTS);
            boolean toCreateMapFile = checkIfNeedMapFileToCreate();
            if (toCreateMapFile) {
                boolean constructStatus = constructMapFile(userContactsFileHDFSRoot);
                if (!constructStatus) {
                    throw new Exception("Failed to construct the map file object for user_contacts files");

                } else {
                    logger.info("Successfully create the map file for user_contacts files");
                }
            } else {
                logger.info("We found an existing user_contacts mapfile {} hence reuse its data.", outputPath);
            }
        }

    }

    boolean constructMapFile(String inputPathRoot) {
        boolean requireProcessed = false;
        long startTime = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder();
        initDB();
        try {
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
            String mapFileOutputPath = hdfsReader.getMapFileOutputPath(ProjectConstant.TASK_MAPFILE_USER_CONTACTS);
            lookupReader = new MapFile.Reader(fs, mapFileOutputPath, config);
        }
        return lookupReader;
    }

    void constructSortedCollection(BufferedReader br) throws IOException {
        /*
        The key is the user id which is mapping to owner_id in user_contacts table
        The value is a composite string. Fields are:
         [ZipCode],[Country],[State],[City]
         */
        int foundExsitingCnt = 0;
        int skipCnt = 0;
        String line;
        line = br.readLine();
        while (line != null) {
            String[] fields = line.split(ProjectConstant.CONTROLA_REC_DELIMITER);
            Long userID = Long.parseLong(fields[ProjectConstant.CONSTANT_USERCONTACTS_OWNERID_POS]);
            //Filter the unneeded records using default_contact and active field
            String defaultFlag = fields[ProjectConstant.CONSTANT_USERCONTACTS_DEFAULTFCONTACT_POS];
            String activeFlag = fields[ProjectConstant.CONSTANT_USERCONTACTS_ACTIVE_POS];
            if (defaultFlag.equals("1") && activeFlag.equals("1")) {
                //Skip the existing keys
                if (treeMap.get(userID) == null) {
                    String city = fields[ProjectConstant.CONSTANT_USERCONTACTS_ADDRCITY_POS];
                    String state = fields[ProjectConstant.CONSTANT_USERCONTACTS_ADDRSTATE_POS];
                    String country = fields[ProjectConstant.CONSTANT_USERCONTACTS_ADDRCOUNTRY_POS];
                    String zipcode = fields[ProjectConstant.CONSTANT_USERCONTACTS_ADDRZIP_POS];
                    String value = zipcode + ProjectConstant.DELIMITER + country
                            + ProjectConstant.DELIMITER + state
                            + ProjectConstant.DELIMITER + city;
                    treeMap.put(userID, value);
                } else {
                    foundExsitingCnt++;
                }
            } else {
                skipCnt++;
            }
            line = br.readLine();
        }
        logger.debug("Found existing {} event records in mapdb database file and skip {} records", foundExsitingCnt, skipCnt);
    }
}
