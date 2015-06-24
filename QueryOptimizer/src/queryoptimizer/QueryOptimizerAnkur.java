/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package queryoptimizer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author ankurpandit
 */
public class QueryOptimizerAnkur {
    public static double pageSize = 4000; //Bytes 
    public static double blockSize = 100; //Pages
    public static double diskAccessTime = 12; //ms
    
    int[][] selectivity = new int[3][3];
    
    public static void main(String[] args) {
        String[] joinMethods = new String[]{"TNL", "PNL", "BNL", "SMJ", "HJM", "HJL"};
        String[] correlationJoinMethods = new String[]{"TNL", "PNL", "BNL"};
        ArrayList<IntializeTableAnkur> tablesInfo = new ArrayList<>();
        tablesInfo.add(new IntializeTableAnkur("T1", 20, 1000));
        tablesInfo.add(new IntializeTableAnkur("T2", 20, 1000));
        tablesInfo.add(new IntializeTableAnkur("T3", 100, 2000));
        double selectivity = 0.01;
        
        Map<String, Double> firstJoin = new HashMap<>();
        Map<String, Double> secondJoin = new HashMap<>();
        for(IntializeTableAnkur firstTable: tablesInfo){
            for(IntializeTableAnkur secondTable: tablesInfo){
                if(firstTable != secondTable){
                    for(String joinMethod: joinMethods){
                        firstJoin.put(joinMethod, calculateJoinFunction(firstTable,secondTable, joinMethod));
                    }
                    
                    IntializeTableAnkur temp1 = new IntializeTableAnkur("temp1", 
                                                                firstTable.getTupleSize()+secondTable.getTupleSize(), 
                                                                selectivity*firstTable.getPageCount()*secondTable.getPageCount());
                    System.out.println("("+firstTable.getTableName()+" join "+secondTable.getTableName()+") => temp1");
                    for(IntializeTableAnkur thirdTable: tablesInfo){
                        if(firstTable != thirdTable && secondTable != thirdTable){
                            System.out.println(temp1.getTableName()+" join "+thirdTable.getTableName());
                            for(String joinMethod: joinMethods){
                                secondJoin.put(joinMethod, calculateJoinFunction(temp1,thirdTable, joinMethod));
                            }
                            System.out.println(thirdTable.getTableName()+" join "+temp1.getTableName());
                            for(String joinMethod: joinMethods){
                                secondJoin.put(joinMethod, calculateJoinFunction(thirdTable,temp1, joinMethod));
                            }
                        }
                    }    
                }
            }    
        }
        System.out.println(firstJoin);
        System.out.println(Collections.min(firstJoin.values()));
        System.out.println(secondJoin);
        System.out.println(Collections.min(secondJoin.values()));
    }
    
    public static double calculateJoinFunction(IntializeTableAnkur leftTable, 
                                        IntializeTableAnkur rightTable, 
                                        String joinMethod){
        double joinCost = 0;
        double joinIO = 0;
        switch(joinMethod){
            case "TNL":
                joinIO = leftTable.getPageCount() + (leftTable.getTuplesCount() * leftTable.getPageCount() * rightTable.getPageCount());
                joinCost = (joinIO * QueryOptimizer.diskAccessTime)/1000;
                break;
            case "NLJ":
                joinIO = leftTable.getPageCount() + (leftTable.getPageCount() * leftTable.getTuplesCount()* 1.2); //1.2: Cost of find matching index tuples
                joinCost = (joinIO * QueryOptimizer.diskAccessTime)/1000;
                break;
            case "PNL":
                joinIO = leftTable.getTuplesCount() + (leftTable.getPageCount() * rightTable.getTuplesCount());
                joinCost = (joinIO * QueryOptimizer.diskAccessTime)/1000;
                break;
            case "BNL":
                joinIO = leftTable.getTuplesCount() + ((leftTable.getPageCount())/QueryOptimizer.blockSize * rightTable.getTuplesCount());
                joinCost = (joinIO * QueryOptimizer.diskAccessTime)/1000;
                break;
            case "SMJ":
                joinIO = 3 * (leftTable.getPageCount() + rightTable.getPageCount());
                joinCost = (joinIO * QueryOptimizer.diskAccessTime)/1000;
                break;
            case "HJM":
                joinIO = 3 * (leftTable.getPageCount() + rightTable.getPageCount());
                joinCost = (joinIO * QueryOptimizer.diskAccessTime)/1000;
                break;
            case "HJL":
                joinIO = 2 * 3 * (leftTable.getPageCount() + rightTable.getPageCount());
                joinCost = (joinIO * QueryOptimizer.diskAccessTime)/1000;
                break;
                
        }
        return joinCost;
    }
}
