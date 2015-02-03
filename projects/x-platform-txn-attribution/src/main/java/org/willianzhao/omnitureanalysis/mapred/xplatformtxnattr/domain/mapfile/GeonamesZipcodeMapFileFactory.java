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
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * This mapfile is to store the post codes lookup table from http://www.geonames.org.
 * Created by weilzhao on 9/28/14.
 */
public class GeonamesZipcodeMapFileFactory extends MapFileFactory {

    DB db;
    /*
    The key will be the combination of country code and zip code contacting by ProjectConstant.DELIMITER.
    The value include fields of lantitude and longtitude.
     */
    Map<String, String> treeMap;
	private static Logger logger = LoggerFactory.getLogger(GeonamesZipcodeMapFileFactory.class);

    public GeonamesZipcodeMapFileFactory(Configuration config) throws IOException {
        this.config = config;
        hdfsReader = new StubHubHDFSReader(config);
        try {
            outputPath = hdfsReader.getMapFileOutputPath(ProjectConstant.TASK_MAPFILE_GEONAMES_ZIPCODE);
            logger.debug("Initial get the output path on {}", outputPath);
        } catch (Exception e) {
            logger.error("Failed to get the output path.");
            //e.printStackTrace(logger.getStream(Level.ERROR));
        }
        this.fs = FileSystem.get(config);
    }


    void initDB() {
        db = DBMaker
                .newFileDB(new File(ProjectConstant.TOKEN_MAPFILE_GEONAMES_ZIPCODE))
                .mmapFileEnable()
                .closeOnJvmShutdown()
                .transactionDisable()
                .cacheSize(1000)
                .make();
        if (db.exists(ProjectConstant.TOKEN_MAPFILE_GEONAMES_ZIPCODE)) {
            treeMap =
                    db.getTreeMap(ProjectConstant.TOKEN_MAPFILE_GEONAMES_ZIPCODE);
        } else {
            treeMap =
                    db.createTreeMap(ProjectConstant.TOKEN_MAPFILE_GEONAMES_ZIPCODE).valuesOutsideNodesEnable()
                            .nodeSize(ProjectConstant.MAPFILE_MAPDB_NODES)
                            .make();
        }
    }

    @Override
    public void loadMapFile() throws Exception {

        if (checkIfMapFileExisted()) {
            logger.info("Skip to create the map file because it's already there");
        } else {
            StubHubHDFSReader hdfsReader = new StubHubHDFSReader(config);
            String resourceFileHDFSRoot = hdfsReader.getMapFileInputPathRoot(ProjectConstant.TASK_MAPFILE_GEONAMES_ZIPCODE);
            boolean constructStatus = constructMapFile(resourceFileHDFSRoot);
            if (!constructStatus) {
                throw new Exception("Failed to construct the map file object for post code files");
            } else {
                logger.info("Successfully create the map file for post code files");
            }
        }
    }

    @Override
    public MapFile.Reader getMapfileReader() throws Exception {
        if (lookupReader == null) {
            StubHubHDFSReader hdfsReader = new StubHubHDFSReader(config);
            String mapFileOutputPath = hdfsReader.getMapFileOutputPath(ProjectConstant.TASK_MAPFILE_GEONAMES_ZIPCODE);
            lookupReader = new MapFile.Reader(fs, mapFileOutputPath, config);
        }
        return lookupReader;
    }

    boolean constructMapFile(String inputPathRoot) {
        long startTime = System.currentTimeMillis();
        initDB();
        Path inputPath = new Path(inputPathRoot);
        try {
            FileStatus[] statusList = fs.listStatus(new Path(inputPathRoot));
            for (FileStatus status : statusList) {
                Path path = status.getPath();
                String fileName = path.getName();
                if (fileName.equals(config.get(ProjectConstant.PATH_ALLCOUNTRY_FILE))) {
                    logger.debug("Processing the all countries zip code file {}", fileName);
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                    constructSortedCollectionForAllCountries(br);
                    logger.info("Finish load the all countries zip code file {}", fileName);
                } else if (fileName.equals(config.get(ProjectConstant.PATH_USZIPCODE_FILE))) {
                    logger.debug("Processing the US zip code file {}", fileName);
                    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
                    constructSortedCollectionForUS(br);
                    logger.info("Finish load the US zip code file {}", fileName);
                } else {
                    logger.debug("The file {} is not in the zipcode lookup scope.Skip it.", fileName);
                }
            }
            long costTime = (System.currentTimeMillis() - startTime) / 1000;
            logger.info("Loading the HDFS files {} \nConstruct memory treemap object totally cost {} seconds",
                    inputPath, costTime);
            logger.info("Start to create the mapfile for geonames file");
            Text key = new Text();
            Text value = new Text();
            writer = new BloomMapFile.Writer(config, fs, outputPath, key.getClass(), value.getClass());
            writer.setIndexInterval(20);
            startTime = System.currentTimeMillis();
            for (Map.Entry<String, String> entry : treeMap.entrySet()) {
                String countryZipcode = entry.getKey();
                String valueStr = entry.getValue();
                key.set(countryZipcode);
                value.set(valueStr);
                writer.append(key, value);
            }
            costTime = (System.currentTimeMillis() - startTime) / 1000;
            logger.info("Successfully created the mapfile. Totally cost {} seconds", costTime);
            return true;
        } catch (IOException e) {
            logger.error("Failed to process the HDFS files under {}", inputPathRoot);
            //e.printStackTrace(logger.getStream(Level.ERROR));

        } finally {
            IOUtils.closeStream(writer);
            db.close();
        }
        return false;
    }

    void constructSortedCollectionForAllCountries(BufferedReader br) throws IOException {

        String line;
        line = br.readLine();
        while (line != null) {
            //The file from geonames.org is tab delimited
            String[] fields = line.split(ProjectConstant.CONSTANT_HITDATA_DELIMITER, -1);
            if (fields.length == ProjectConstant.CONSTANT_GEONAMESZIP_TOTAL_FIELDS_CNT) {
                String countryCode = fields[ProjectConstant.CONSTANT_GEONAMESZIP_COUNTRYCODE_POS];
                String zipCode = fields[ProjectConstant.CONSTANT_GEONAMESZIP_ZIPCODE_POS];
                String key = countryCode + ProjectConstant.CONSTANT_HITDATA_DELIMITER + zipCode;
                //Skip the existing keys
                if (treeMap.get(key) == null) {
                    String latitude = fields[ProjectConstant.CONSTANT_GEONAMESZIP_LATI_POS];
                    String longitude = fields[ProjectConstant.CONSTANT_GEONAMESZIP_LONG_POS];
                    String value = latitude + ProjectConstant.CONSTANT_HITDATA_DELIMITER + longitude;
                    treeMap.put(key, value);
                }
            }
            line = br.readLine();
        }
    }

    void constructSortedCollectionForUS(BufferedReader br) throws IOException {
        String line;
        line = br.readLine();
        while (line != null) {
            //The file from UPS is comma delimited and double quotes quoted.
            String[] fields = line.split(ProjectConstant.DELIMITER_COMMA, -1);
            if (fields.length == ProjectConstant.CONSTANT_USZIP_TOTAL_FIELDS_CNT) {
                String countryCode = "US";
                String zipCode = fields[ProjectConstant.CONSTANT_USZIP_ZIPCODE_POS].replace(ProjectConstant.QUOTED_CHAR, "");
                String key = countryCode + ProjectConstant.CONSTANT_HITDATA_DELIMITER + zipCode;
                //Skip the existing keys
                if (treeMap.get(key) == null) {
                    String latitude = fields[ProjectConstant.CONSTANT_USZIP_LATITUDE_POS].replace(ProjectConstant.QUOTED_CHAR, "");
                    String longitude = fields[ProjectConstant.CONSTANT_USZIP_LONGITUDE_POS].replace(ProjectConstant.QUOTED_CHAR, "");
                    String value = latitude + ProjectConstant.CONSTANT_HITDATA_DELIMITER + longitude;
                    treeMap.put(key, value);
                }
            }
            line = br.readLine();
        }
    }
}
