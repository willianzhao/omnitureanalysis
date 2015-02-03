package org.willianzhao.omnitureanalysis.mapred.commons.misc.timezonehelper.shapefile;

/**
 * From http://www.esri.com/library/whitepapers/pdfs/shapefile.pdf Note only
 * NullShape and Polygon are used in the minimal implementation in ShpFile.
 *
 * @author Frank D. Russo
 */
public enum ShapeType {
    NullShape(0),
    Point(1),
    PolyLine(3),
    Polygon(5),
    MultiPoint(8),
    PointZ(11),
    PolyLineZ(13),
    PolygonZ(15),
    MultiPointZ(18),
    PointM(21),
    PolyLineM(23),
    PolygonM(25),
    MultiPointM(28),
    MultiPatch(31),
    ;

    private int typeCode;
    private ShapeType(int typeCode) {
        this.typeCode = typeCode;
    }

    public static ShapeType forCode(int typeCode) {
        for (ShapeType shapeType : values())
            if (shapeType.typeCode == typeCode)
                return shapeType;

        throw new IllegalArgumentException("No Shape Type specified for code " + typeCode);
    }
}
