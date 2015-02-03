package org.willianzhao.omnitureanalysis.mapred.commons.model;

public class GenreHierarchy extends GenreTreeAbstract {
    String level1GenreID = "";
    String level1GenreDesc = "";
    String level2GenreID = "";
    String level2GenreDesc = "";
    String level3GenreID = "";
    String level3GenreDesc = "";
    String level4GenreID = "";
    String level4GenreDesc = "";
    String level5GenreID = "";
    String level5GenreDesc = "";
    String level6GenreID = "";
    String level6GenreDesc = "";
    String top2ndGenreDesc ="";
    String gcf="";

    public GenreHierarchy(String level1GenreID) {  	
        this.level1GenreID = level1GenreID;
    }

    public String getLevel1GenreID() {
        return level1GenreID;
    }

    public void setLevel1GenreID(String level1GenreID) {
        this.level1GenreID = level1GenreID;
    }

    public String getLevel1GenreDesc() {
        return level1GenreDesc;
    }

    public void setLevel1GenreDesc(String level1GenreDesc) {
        this.level1GenreDesc = level1GenreDesc;
    }

    public String getLevel2GenreID() {
        return level2GenreID;
    }

    public void setLevel2GenreID(String level2GenreID) {
        this.level2GenreID = level2GenreID;
    }

    public String getLevel2GenreDesc() {
        return level2GenreDesc;
    }

    public void setLevel2GenreDesc(String level2GenreDesc) {
        this.level2GenreDesc = level2GenreDesc;
    }

    public String getLevel3GenreID() {
        return level3GenreID;
    }

    public void setLevel3GenreID(String level3GenreID) {
        this.level3GenreID = level3GenreID;
    }

    public String getLevel3GenreDesc() {
        return level3GenreDesc;
    }

    public void setLevel3GenreDesc(String level3GenreDesc) {
        this.level3GenreDesc = level3GenreDesc;
    }

    public String getLevel4GenreID() {
        return level4GenreID;
    }

    public void setLevel4GenreID(String level4GenreID) {
        this.level4GenreID = level4GenreID;
    }

    public String getLevel4GenreDesc() {
        return level4GenreDesc;
    }

    public void setLevel4GenreDesc(String level4GenreDesc) {
        this.level4GenreDesc = level4GenreDesc;
    }

    public String getLevel5GenreID() {
        return level5GenreID;
    }

    public void setLevel5GenreID(String level5GenreID) {
        this.level5GenreID = level5GenreID;
    }

    public String getLevel5GenreDesc() {
        return level5GenreDesc;
    }

    public void setLevel5GenreDesc(String level5GenreDesc) {
        this.level5GenreDesc = level5GenreDesc;
    }

    public String getLevel6GenreID() {
        return level6GenreID;
    }

    public void setLevel6GenreID(String level6GenreID) {
        this.level6GenreID = level6GenreID;
    }

    public String getLevel6GenreDesc() {
        return level6GenreDesc;
    }

    public void setLevel6GenreDesc(String level6GenreDesc) {
        this.level6GenreDesc = level6GenreDesc;
    }

    public String getTop2ndGenreDesc() {

        if(level4GenreID.trim().length()==0){
            //There are only 3 levels of genre hierarchy
            top2ndGenreDesc=level3GenreDesc;
        }else if(level5GenreID.trim().length()>0 && level6GenreID.trim().length()==0){
            //There are more than 6 levels of genre hierarchy
            top2ndGenreDesc=level3GenreDesc;
        }else if (level6GenreID.trim().length()>0){
            //Give a default value
            top2ndGenreDesc=level4GenreDesc;
        }else{
            top2ndGenreDesc=level3GenreDesc;
        }
        return top2ndGenreDesc;
    }

    public String getGcf() {
        return gcf;
    }

    public void setGcf(String gcf) {
        this.gcf = gcf;
    }
}
