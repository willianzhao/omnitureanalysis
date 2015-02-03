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
 * Created by willianzhao on 6/8/14.
 */
public class UserMapFileFactory extends MapFileFactory {

    DB db;
    Map<Long, String> treeMap;

	private static Logger logger = LoggerFactory.getLogger(UserMapFileFactory.class);

    public UserMapFileFactory(Configuration conf) throws IOException {
        super(conf);
        hdfsReader = new StubHubHDFSReader(config);
        try {
            outputPath = hdfsReader.getMapFileOutputPath(ProjectConstant.TASK_MAPFILE_USERS);
        } catch (Exception e) {
            //do nothing
        }
        this.fs = FileSystem.get(config);
    }

    void initDB() {

        db = DBMaker
                .newFileDB(new File(ProjectConstant.TOKEN_MAPFILE_USERS))
                .mmapFileEnable()
                .closeOnJvmShutdown()
                .transactionDisable()
                .cacheSize(1000)
                .make();

        if (db.exists(ProjectConstant.TOKEN_MAPFILE_USERS)) {
            treeMap =
                    db.getTreeMap(ProjectConstant.TOKEN_MAPFILE_USERS);
        } else {
            treeMap =
                    db.createTreeMap(ProjectConstant.TOKEN_MAPFILE_USERS).valuesOutsideNodesEnable()
                            .nodeSize(ProjectConstant.MAPFILE_MAPDB_NODES)
                            .make();
        }
    }

    @Override
    public void loadMapFile() throws Exception {
        if (writer == null) {
            StubHubHDFSReader hdfsReader = new StubHubHDFSReader(config);
            String usersFileHDFSRoot = hdfsReader.getMapFileInputPathRoot(ProjectConstant.TASK_MAPFILE_USERS);
            if (usersFileHDFSRoot == null || usersFileHDFSRoot.trim().length() == 0) {
                throw new Exception("The user file hdfs path root is missing in configure file");
            }
            String outputPath = hdfsReader.getMapFileOutputPath(ProjectConstant.TASK_MAPFILE_USERS);
            boolean toCreateMapFile = checkIfNeedMapFileToCreate();
            if (toCreateMapFile) {
                boolean constructStatus = constructMapFile(usersFileHDFSRoot);
                if (!constructStatus) {
                    throw new Exception("Failed to construct the map file object for users files");
                } else {
                    logger.info("Successfully create the map file for users files");
                }
            } else {
                logger.info("We found an existing user mapfile {} hence reuse its data.", outputPath);
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
                        logger.info("Exclude Path {} from the valid scope.", path);
                    }
                    recordMaxProcessedCal(path);
                } else {
                    logger.error("The path {} is not directory but file. Skip it.", path);
                }
            }
            long costTime = (System.currentTimeMillis() - startTime) / 1000;
            logger.info("Reading the HDFS files and then construct memory treemap object totally cost {} seconds",
                    costTime);
            logger.info("Start to create the mapfile for users");
            LongWritable key = new LongWritable();
            Text value = new Text();
            writer = new BloomMapFile.Writer(config, fs, outputPath, key.getClass(), value.getClass());
            writer.setIndexInterval(20);
            startTime = System.currentTimeMillis();
            for (Map.Entry<Long, String> entry : treeMap.entrySet()) {
                Long userID = entry.getKey();
                String userValue = entry.getValue();
                key.set(userID);
                value.set(userValue);
                writer.append(key, value);
            }
            costTime = (System.currentTimeMillis() - startTime) / 1000;
            logger.info("Successfully created the mapfile. Totally cost {} seconds", costTime);
            markTimestamp(outputPath);
            return true;
        } catch (IOException e) {
            logger.error("Failed to process the HDFS files under {}", inputPathRoot);
           // e.printStackTrace(logger.getStream(Level.ERROR));
        } catch (ParseException e) {
           // e.printStackTrace(logger.getStream(Level.ERROR));
        } finally {
            IOUtils.closeStream(writer);
            db.close();
        }
        return false;
    }

    void constructSortedCollection(BufferedReader br) throws IOException {
        String line;
        line = br.readLine();
        Long userID = null;
        String userValue = null;
        while (line != null) {
            String[] fields = line.split(ProjectConstant.CONTROLA_REC_DELIMITER);
            //Get the 1st and 2nd for ticketID and userID
            userID = Long.parseLong(fields[ProjectConstant.CONSTANT_USERS_USERID_POS]);

            /*
            The user value is :

            [date_joined],[guid no#],[guid 1],[guid 2],...
             */
            String date_joined = fields[ProjectConstant.CONSTANT_USERS_DATEJOIN_POS];
            String guid = fields[ProjectConstant.CONSTANT_USERS_GUIDID_POS];
            String priorValue = treeMap.get(userID);
            if (priorValue == null || priorValue.trim().length() == 0) {
                //New record
                if (guid == null || guid.trim().length() < ProjectConstant.CONSTANT_GUID_LENGTH) {
                    userValue = date_joined + ProjectConstant.DELIMITER + "0";
                } else {
                    userValue = date_joined + ProjectConstant.DELIMITER + "1" + ProjectConstant.DELIMITER + guid;
                }
                treeMap.put(userID, userValue);

            } else {
                //Existing record
                String[] priorValues = priorValue.split(ProjectConstant.DELIMITER);
                int priorFieldNo = priorValues.length;
                if (priorFieldNo < 2) {

                    /*
                    There is something wrong for prior value. Use the new value to overwrite it
                     */
                    if (guid == null || guid.trim().length() < ProjectConstant.CONSTANT_GUID_LENGTH) {
                        userValue = date_joined + ProjectConstant.DELIMITER + "0";
                    } else {
                        userValue = date_joined + ProjectConstant.DELIMITER + "1" + ProjectConstant.DELIMITER + guid;
                    }
                    treeMap.put(userID, userValue);

                } else {

                    /*
                    find the existing valid user value
                     */
                    String guidCntStr = priorValues[1];
                    int guidCnt = Integer.parseInt(guidCntStr);
                    //Reuse the prior created date
                    date_joined = priorValues[0];
                    if (guidCnt == 0) {
                        /*
                        There is no guid in prior record
                         */
                        if (guid != null && guid.trim().length() >= ProjectConstant.CONSTANT_GUID_LENGTH) {
                            userValue = date_joined + ProjectConstant.DELIMITER + "1" + ProjectConstant.DELIMITER + guid;
                            treeMap.put(userID, userValue);
                        }
                    } else {

                /*
                If there are multiple user records with different guid, then concat them into one line
                Skip the existing guid
                 */
                        if (guid != null && guid.trim().length() >= ProjectConstant.CONSTANT_GUID_LENGTH) {
                            userValue = date_joined + ProjectConstant.DELIMITER + (guidCnt + 1);
                            boolean foundDup = false;
                            for (int i = 0; i < guidCnt; i++) {
                                final int position = i + 2;
                                String legacyGUID = priorValues[position];
                                if (legacyGUID.equals(guid)) {
                                    foundDup = true;
                                }
                                userValue = userValue + ProjectConstant.DELIMITER + legacyGUID;
                            }
                            if (!foundDup) {
                                //The guid is new for the existing user record. This is to avoid insert duplicate guid
                                userValue = userValue + ProjectConstant.DELIMITER + guid;
                                treeMap.put(userID, userValue);
                            }
                        }

                    }

                }
            }
            line = br.readLine();
        }

    }

    @Override
    public MapFile.Reader getMapfileReader() throws Exception {
        if (lookupReader == null) {
            StubHubHDFSReader hdfsReader = new StubHubHDFSReader(config);
            String mapFileOutputPath = hdfsReader.getMapFileOutputPath(ProjectConstant.TASK_MAPFILE_USERS);
            lookupReader = new MapFile.Reader(fs, mapFileOutputPath, config);
        }
        return lookupReader;
    }
}
