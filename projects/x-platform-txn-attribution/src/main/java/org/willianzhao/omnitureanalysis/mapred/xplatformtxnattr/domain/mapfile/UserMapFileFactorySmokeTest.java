package org.willianzhao.omnitureanalysis.mapred.xplatformtxnattr.domain.mapfile;

import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Created by willianzhao on 6/17/14.
 */
public class UserMapFileFactorySmokeTest {

    Configuration configure;
    MapFile.Reader userMapFileReader;

	private static Logger logger = LoggerFactory.getLogger(UserMapFileFactorySmokeTest.class);

    @Before
    public void setupEnv() {
        configure = new Configuration();
        UserMapFileFactory userMapFile = null;
        try {
            userMapFile = new UserMapFileFactory(configure);
            userMapFileReader = userMapFile.getMapfileReader();

        } catch (ParseException e) {
           // e.printStackTrace(logger.getStream(Level.ERROR));
        } catch (Exception e) {
           // e.printStackTrace(logger.getStream(Level.ERROR));
        }

    }

    @Test
    public void testUserRegisterTime() throws IOException {
        String userID = "186891780270";
        String expectedTime = "2014-02-21 23:35:59";
        String userRegisteTime;
        LongWritable ecommUserIDKey = new LongWritable(Long.parseLong(userID));
        Text userValue = new Text();
        Writable val = userMapFileReader.get(ecommUserIDKey, userValue);
        Text temp = val != null ? (Text) val : null;
        if (temp != null) {
            String tempValue = temp.toString();
//            System.out.println("userid = " + userID + ", user value = '" + tempValue + "'");
            String[] userInfoList = tempValue.split(ProjectConstant.DELIMITER);
            if (userInfoList.length >= 2) {
                userRegisteTime = userInfoList[0];
                assertEquals("Validate the register time in the value of users mapfile '" + tempValue + "'",
                        expectedTime, userRegisteTime);
            }

        }
    }

    @Test
    public void testUserGuidCnt() throws IOException {
        String userID = "186891780270";
        LongWritable ecommUserIDKey = new LongWritable(Long.parseLong(userID));
        Text userValue = new Text();
        Writable val = userMapFileReader.get(ecommUserIDKey, userValue);
        Text temp = val != null ? (Text) val : null;
        if (temp != null) {
            String tempValue = temp.toString();
//            System.out.println("userid = " + userID + ", user value = '" + tempValue + "'");
            String[] userInfoList = tempValue.split(ProjectConstant.DELIMITER);
            if (userInfoList.length >= 2) {
                String guidCnt = userInfoList[1];
                assertNotEquals(
                        "Validate the no of guid in the value of users mapfile '" + tempValue + "'. There should be one guid for this user.",
                        guidCnt, "0");

            }
        }

    }

    @Test
    public void testUserGuidValue() throws IOException {
        String userID = "186891780270";
        String expectGuid = "F32C9E7FEFF025B1E04400212864D3F7";
        String guid;
        LongWritable ecommUserIDKey = new LongWritable(Long.parseLong(userID));
        Text userValue = new Text();
        Writable val = userMapFileReader.get(ecommUserIDKey, userValue);
        Text temp = val != null ? (Text) val : null;
        if (temp != null) {
            String tempValue = temp.toString();
//            System.out.println("userid = " + userID + ", user value = '" + tempValue + "'");
            String[] userInfoList = tempValue.split(ProjectConstant.DELIMITER);
            if (userInfoList.length >= 2) {
                String guidCnt = userInfoList[1];
                if (!guidCnt.equals("0")) {
                    guid = userInfoList[2];
                    assertEquals("Validate the guid in the value of users mapfile '" + tempValue + "'", expectGuid,
                            guid);

                }

            }

        }
    }

    @After
    public void tearDown() throws IOException {
        userMapFileReader.close();
    }
}
