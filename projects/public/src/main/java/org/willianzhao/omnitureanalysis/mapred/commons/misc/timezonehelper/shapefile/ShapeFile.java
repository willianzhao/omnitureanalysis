package org.willianzhao.omnitureanalysis.mapred.commons.misc.timezonehelper.shapefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Minimal implementation of a parser for the shapefile standard
 * (http://www.esri.com/library/whitepapers/pdfs/shapefile.pdf), sufficient to
 * parse the polygon shapefile from tz_world (http://efele.net/maps/tz/world/).
 * A "shapefile" foo consists of four actual files: foo.shp, foo.shx, foo.dbf
 * and foo.prj. The .shp file is parsed by ShpFile, and the .dbf file is parsed
 * by DbfFile; the others are ignored.
 *
 * @author Frank D. Russo
 *
 * Change on Sep 28 2014 by Willian Zhao
 *
 * Change the constructor by reading from HDFS
 */
public class ShapeFile {

    FSDataInputStream shpFileIn;
    FSDataInputStream dbfFileIn;
    private ShpFile shpFile;
    private DbfFile dbfFile;

    public ShapeFile(Configuration conf, String shapeFileRoot, String name) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        // main shape geometry file
        String shpUrl = shapeFileRoot + "/" + name + ".shp";
        Path shpPath = new Path(shpUrl);
        if (fs.exists(shpPath)) {
            shpFileIn = fs.open(shpPath);
            shpFile = new ShpFile(shpFileIn);
            // ignore .shx file, since we'll be reading the whole shapefile
            // metadata in dbf file
            String dbfUrl = shapeFileRoot + "/" + name + ".dbf";
            Path dbfPath = new Path(dbfUrl);
            dbfFileIn = fs.open(dbfPath);
            dbfFile = new DbfFile(dbfFileIn);
            // ignore .prj file with coordinate system - for tz_world we know this is the global lat-lon system
        }
    }

    public ShapeFileShape readShape() throws IOException {
        // shp file provides actual shape data
        ShapeFileShape shape = shpFile.readShape();
        if (shape == null) return null; // EOF
        // dbf file is synced with shp file
        shape.setShapeMetadata(dbfFile.readRecord());
        return shape;
    }

    public void close() throws IOException {
        shpFile.close();
        dbfFile.close();
        shpFileIn.close();
        dbfFileIn.close();
    }
}
