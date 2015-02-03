package org.willianzhao.omnitureanalysis.mapred.commons.model;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

import org.willianzhao.omnitureanalysis.mapred.commons.misc.ProjectConstant;


public class GenreHierarchyAnalyzer {

	HashMap<String, HashMap<Integer, GenreNode>> genreLeafMap;
    Configuration config;
    Context context;
    GenreTreeAbstract genreTree;
    String firstLevelGenreID;
    
   
    public GenreHierarchyAnalyzer(Context context,String level1GenreID,HashMap<String, HashMap<Integer, GenreNode>> genreLeafMap,GenreTreeAbstract genreTree) {
    	this.config =context.getConfiguration();
    	this.context =context;
        this.genreLeafMap = genreLeafMap;
        this.genreTree =  genreTree;
        this.firstLevelGenreID = level1GenreID;
    }
    
    public  boolean fillGenreHierarchy() {
        if (firstLevelGenreID != null && firstLevelGenreID.length() > 0) {
            if (genreLeafMap != null) {
                String gcfID;
                HashMap<Integer, GenreNode> genreLevelMap = genreLeafMap.get(firstLevelGenreID);
                if (genreLevelMap != null) {
                    String genreDescTemp;
                    String genreIDTemp;
                    GenreNode gn = genreLevelMap.get(1);
                    if (gn != null) {
                        genreDescTemp = gn.getGenreDescription();
                        genreTree.setLevel1GenreDesc(genreDescTemp);
                        /*
                        First try to get genre cut final for the genre level 1.
                        Because all the genre in the same hierarchy have the same gcf.
                         */
                        
                        gcfID = gn.getGcfID();
                        System.out.println(" methd gcfID at level 1:"+gcfID);
                        
                        if (gcfID != null && gcfID.trim().length() != 0) {
                            String gcf = config.get(ProjectConstant.GENRE_CUT_FINAL_CONF_PREFIX + gcfID);
                            System.out.println("level 1 gcf :"+gcf);
                            if (gcf != null) {
                                genreTree.setGcf(gcf);
                            }else{
                                gcfID=ProjectConstant.CONSTANT_UNFOUND_FLAG;
                                System.out.println("level 1 gcf not found");
                            }
                        } else {
                            gcfID = ProjectConstant.CONSTANT_UNFOUND_FLAG;
                            System.out.println("level 1 gcf not found");
                        }
                    } else {
                        genreTree.setLevel1GenreDesc("");
                        //for debug
                      //RM context.getCounter("Debug", "Failed to get the level 1 genre description").increment(1);
                        return false;
                    }
                    gn = genreLevelMap.get(2);
                    if (gn != null) {
                        genreDescTemp = gn.getGenreDescription();
                        genreIDTemp = gn.getGenreID();
                        genreTree.setLevel2GenreID(genreIDTemp);
                        genreTree.setLevel2GenreDesc(genreDescTemp);
                        if (gcfID.equals(ProjectConstant.CONSTANT_UNFOUND_FLAG)) {
                        /*
                        If genre level 1 doesn't have the gcfID then try to get genre cut final for the genre level 2.
                        Because all the genre in the same hierarchy have the same gcf.
                         */
                            System.out.println(" methd gcfID at level 2:"+gcfID);
                            gcfID = gn.getGcfID();
                            if (gcfID != null && gcfID.trim().length() != 0) {
                                String gcf = config.get(ProjectConstant.GENRE_CUT_FINAL_CONF_PREFIX + gcfID);
                                System.out.println("level 2 gcf :"+gcf);
                                if (gcf != null) {
                                    genreTree.setGcf(gcf);
                                }else{
                                    gcfID=ProjectConstant.CONSTANT_UNFOUND_FLAG;
                                }
                            } else {
                                gcfID = ProjectConstant.CONSTANT_UNFOUND_FLAG;
                            }
                        }
                    } else {
                        //From the second level of genre, if no genre level existed, return true;
                        return true;
                    }
                    gn = genreLevelMap.get(3);
                    if (gn != null) {
                        genreDescTemp = gn.getGenreDescription();
                        genreIDTemp = gn.getGenreID();
                        genreTree.setLevel3GenreID(genreIDTemp);
                        genreTree.setLevel3GenreDesc(genreDescTemp);
                        if (gcfID.equals(ProjectConstant.CONSTANT_UNFOUND_FLAG)) {
                        /*
                        If genre level 2 doesn't have the gcfID then try to get genre cut final for the genre level 3.
                        Because all the genre in the same hierarchy have the same gcf.
                         */
                            System.out.println(" methd gcfID at level 3:"+gcfID);
                                                      gcfID = gn.getGcfID();
                            if (gcfID != null && gcfID.trim().length() != 0) {
                                String gcf = config.get(ProjectConstant.GENRE_CUT_FINAL_CONF_PREFIX + gcfID);
                                System.out.println("level 3 gcf :"+gcf);
                                if (gcf != null) {
                                    genreTree.setGcf(gcf);
                                }else{
                                    gcfID=ProjectConstant.CONSTANT_UNFOUND_FLAG;
                                }
                            } else {
                                gcfID = ProjectConstant.CONSTANT_UNFOUND_FLAG;
                            }
                        }
                    } else {
                        return true;
                    }
                    gn = genreLevelMap.get(4);
                    if (gn != null) {
                        genreDescTemp = gn.getGenreDescription();
                        genreIDTemp = gn.getGenreID();
                        genreTree.setLevel4GenreID(genreIDTemp);
                        genreTree.setLevel4GenreDesc(genreDescTemp);
                        if (gcfID.equals(ProjectConstant.CONSTANT_UNFOUND_FLAG)) {
                        /*
                        If genre level 3 doesn't have the gcfID then try to get genre cut final for the genre level 4.
                        Because all the genre in the same hierarchy have the same gcf.
                         */
                            gcfID = gn.getGcfID();
                            System.out.println(" methd gcfID at level 4:"+gcfID);
                            if (gcfID != null && gcfID.trim().length() != 0) {
                                String gcf = config.get(ProjectConstant.GENRE_CUT_FINAL_CONF_PREFIX + gcfID);
                                System.out.println("level 4 gcf :"+gcf);
                                if (gcf != null) {
                                    genreTree.setGcf(gcf);
                                }else{
                                    gcfID=ProjectConstant.CONSTANT_UNFOUND_FLAG;
                                }
                            } else {
                                gcfID = ProjectConstant.CONSTANT_UNFOUND_FLAG;
                            }
                        }
                    } else {
                        return true;
                    }
                    gn = genreLevelMap.get(5);
                    if (gn != null) {
                        genreDescTemp = gn.getGenreDescription();
                        genreIDTemp = gn.getGenreID();
                        genreTree.setLevel5GenreID(genreIDTemp);
                        genreTree.setLevel5GenreDesc(genreDescTemp);
                        if (gcfID.equals(ProjectConstant.CONSTANT_UNFOUND_FLAG)) {
                        /*
                        If genre level 4 doesn't have the gcfID then try to get genre cut final for the genre level 5.
                        Because all the genre in the same hierarchy have the same gcf.
                         */
                            gcfID = gn.getGcfID();
                            System.out.println(" methd gcfID at level 5:"+gcfID);
                           if (gcfID != null && gcfID.trim().length() != 0) {
                                String gcf = config.get(ProjectConstant.GENRE_CUT_FINAL_CONF_PREFIX + gcfID);
                                System.out.println("level 5 gcf :"+gcf);
                               if (gcf != null) {
                                    genreTree.setGcf(gcf);
                                }
                            }
                        }
                    } else {
                        return true;
                    }
                    gn = genreLevelMap.get(6);
                    if (gn != null) {
                        genreDescTemp = gn.getGenreDescription();
                        genreIDTemp = gn.getGenreID();
                        genreTree.setLevel6GenreID(genreIDTemp);
                        genreTree.setLevel6GenreDesc(genreDescTemp);

                    } else {
                        return true;
                    }
                    return true;

                }

            } else {
                //for debug
            	//RM context.getCounter("Debug", "genreLeafMap is null").increment(1);
            }

        }
        return false;
    }

}
