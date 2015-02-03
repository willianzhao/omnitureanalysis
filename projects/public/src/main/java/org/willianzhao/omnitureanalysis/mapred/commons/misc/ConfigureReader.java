package org.willianzhao.omnitureanalysis.mapred.commons.misc;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Properties;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.willianzhao.omnitureanalysis.mapred.commons.dm.GenreIdentifier;

/**
 * Created by weilzhao on 3/17/2014.
 */
public class ConfigureReader {

	private static Logger logger = LoggerFactory.getLogger(GenreIdentifier.class);

	
    public Properties getLocalConfProperties(String fileName) throws InvalidParameterException, IOException {
        Reader reader = null;
        Properties properties = new Properties();
        try {
            try {
                reader = new FileReader(fileName);
            } catch (FileNotFoundException e) {
                InputStream inputStream = ConfigureReader.class.getResourceAsStream("/" + fileName);
                if (inputStream == null) {
                    logger.error("Fail to find the configure file " + fileName + " in the place of class");
                    throw new FileNotFoundException(fileName);
                }
                reader = new InputStreamReader(inputStream);

            } catch (InvalidParameterException ipe) {
                logger.error(ipe.getMessage());
                throw new InvalidParameterException();
            }
            properties.load(reader);

        } finally {
            if (reader != null) {
                reader.close();

            }
        }
        return properties;
    }

    public String getConfigValue(String key) throws IOException {
        Properties p = null;
        p = getLocalConfProperties(ProjectConstant.CONF_HITDATA);
        // Setup the lookup server host
        String configItem = p.getProperty(key).trim();
        return configItem;

    }

    public HashMap<String, Integer> mappingColumns() throws IOException {
        HashMap<String, Integer> requiredColsPosition = new HashMap<String, Integer>();
        Properties p = null;
        // Initial the Columns map
        p = getLocalConfProperties(ProjectConstant.CONF_HITDATA);
        System.out.println(ProjectConstant.CONF_HITDATA);
        String requiredCols = p.getProperty("REQUIRED_COLS");
        System.out.println(requiredCols);
        StringTokenizer requiredColsIterator = new StringTokenizer(requiredCols, ",");
        while (requiredColsIterator.hasMoreTokens()) {
            String colNameIndex = requiredColsIterator.nextToken();
            if (colNameIndex != null && colNameIndex.length() > 0) {
                requiredColsPosition.put(p.getProperty(colNameIndex).trim(), -1);
                System.out.println("Index "+ colNameIndex+"="+p.getProperty(colNameIndex).trim());

            }
        }
        String cachedLine = null;
        try {
            // setup a BufferedReader for file stored at location confPath
        	 System.out.println("ProjectConstant.CONF_COLUMNHEADER"+ProjectConstant.CONF_COLUMNHEADER);
            BufferedReader reader = new BufferedReader(new FileReader(ProjectConstant.CONF_COLUMNHEADER));
            // string to hold each line from the cached file
            cachedLine = reader.readLine();
            System.out.println("cachedLine"+cachedLine);

        } catch (FileNotFoundException e) {
            InputStream inputStream = ConfigureReader.class.getResourceAsStream("/" + ProjectConstant.CONF_COLUMNHEADER);
            
            if (inputStream == null) {
            	 System.out.println("inputStream is NULL");
                logger.error("Fail to find the column header file " + ProjectConstant.CONF_COLUMNHEADER + " in the place of class");
                throw new FileNotFoundException(ProjectConstant.CONF_COLUMNHEADER);
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            cachedLine = reader.readLine();
            System.out.println("cachedLine"+cachedLine);

        }
        // position starts from 0
        int pos = 0;
        // iterate through each line of the file stored in Distributed Cache
        if (cachedLine != null && cachedLine.length() > 0) {
            // tokenize each line on "\t"
            StringTokenizer cachedIterator = new StringTokenizer(cachedLine, "\t");
            while (cachedIterator.hasMoreTokens()) {
                String colName = cachedIterator.nextToken();
                if (requiredColsPosition.containsKey(colName)) {
                    requiredColsPosition.put(colName, pos);
                    System.out.println("colName :pos"+colName +":" +pos);

                }
                // field position increase one
                pos = pos + 1;
            }
        } else {
            logger.error("Cached header file contains none line");
        }
        return requiredColsPosition;
    }

}
