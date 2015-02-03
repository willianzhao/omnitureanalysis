package org.willianzhao.omnitureanalysis.mapred.commons.model;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by willianzhao on 5/7/14.
 */
public class GenreNode implements WritableComparable {

    String genreID;
    String parentID;
    String genreDescription = "";
    String gcfID;

    public GenreNode() {
    }

    public GenreNode(String genreID, String parentID, String genreDescription,String gcfID) {
        this.genreID = genreID;
        this.parentID = parentID;
        this.genreDescription = genreDescription;
        this.gcfID=gcfID;
    }

    public String getGenreID() {
        return genreID;
    }

    public void setGenreID(String genreID) {
        this.genreID = genreID;
    }

    public String getParentID() {
        return parentID;
    }

    public void setParentID(String parentID) {
        this.parentID = parentID;
    }

    public String getGenreDescription() {
        return genreDescription;
    }

    public void setGenreDescription(String genreDescription) {
        this.genreDescription = genreDescription;
    }

    public String getGcfID() {
        return gcfID;
    }

    public void setGcfID(String gcfID) {
        this.gcfID = gcfID;
    }

    @Override
    public String toString() {
        return
                genreID +
                        "," + parentID +
                        ",'" + genreDescription +
                        "'" + gcfID
                ;
    }

    @Override
    public int hashCode() {
        return genreID.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof GenreNode)) {
            return false;
        }
        GenreNode otherGenre = (GenreNode) o;
        return genreID.equals(otherGenre.getGenreID());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, genreID);
        Text.writeString(out, parentID);
        Text.writeString(out, genreDescription);
        Text.writeString(out, gcfID);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.genreID = Text.readString(in);
        this.parentID = Text.readString(in);
        this.genreDescription = Text.readString(in);
        this.gcfID= Text.readString(in);
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof GenreNode) {
            GenreNode other = (GenreNode) o;
            return this.getGenreID().compareTo(other.getGenreID());
        } else {
            return 1;
        }
    }
}
