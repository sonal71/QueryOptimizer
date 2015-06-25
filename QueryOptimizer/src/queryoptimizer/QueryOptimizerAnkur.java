/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package queryoptimizer;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

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
        try{
            File file = new File("src/queryoptimizer/inputAnkur.txt");
            Scanner input = new Scanner(file);
            String line, firstTable, secondTable, resultTable, selectivity, source_table;
            int co_flag;
            String[] params;
            while(input.hasNextLine()) {
                line = input.nextLine();
                params = line.split(" ");
                switch(params[0]) {
                    case "Q": // Starting of new query
//                        System.out.println("Query");
                        break;
                    case "J": // Joins
                        firstTable = params[1];
                        secondTable = params[2];
                        resultTable = params[3];
                        co_flag = Integer.parseInt(params[4]);
                        selectivity = params[5];
                        if(co_flag == 1) {
                            calculateJoinFunctionCorrelation(firstTable, secondTable, resultTable, selectivity);
                        }else {
                            calculateJoinFunction(firstTable, secondTable, resultTable, selectivity);
                        }
                        // Call join method using above mentioned parameters
                        //System.out.println("Join "+firstTable+" "+secondTable+" "+resultTable+" "+co_flag+" "+selectivity);
                        break;
                    case "P": // Projection
                        //call cost caliculation for projection
//                        System.out.println("Projection");
                        break;
                    case "G": //Group By
                        source_table = params[1];
                        //call cost caliculation for group by
                        break;
                    case "#": //To indicate starting of query
//                        System.out.println("Query Starts");
                        break;
                    case "##": //To indicate ending of query
//                        System.out.println("Query Ends");
                        break;
                    default:
//                        System.out.println("others");
                        break;
                }
            }
        }catch(FileNotFoundException f) {
            System.out.println("File Not Found");
        }
        System.exit(0);
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
                                                                firstTable.tupleSize+secondTable.tupleSize, 
                                                                selectivity*firstTable.pageCount*secondTable.pageCount);
                    System.out.println("("+firstTable.tableName+" join "+secondTable.tableName+") => temp1");
                    for(IntializeTableAnkur thirdTable: tablesInfo){
                        if(firstTable != thirdTable && secondTable != thirdTable){
                            System.out.println(temp1.tableName+" join "+thirdTable.tableName);
                            for(String joinMethod: joinMethods){
                                secondJoin.put(joinMethod, calculateJoinFunction(temp1,thirdTable, joinMethod));
                            }
                            System.out.println(thirdTable.tableName+" join "+temp1.tableName);
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
    
    public static double calculateJoinFunctionCorrelation(IntializeTableAnkur leftTable, 
                                        IntializeTableAnkur rightTable, 
                                        String joinMethod, String s){
        double joinCost = 0;
        double joinIO = 0;
        double selectivity = Double.parseDouble(s);
        switch(joinMethod){
            case "TNL":
                joinIO = leftTable.pageCount + (leftTable.tupleCount * leftTable.pageCount * rightTable.pageCount);
                joinCost = (joinIO * QueryOptimizer.diskAccessTime)/1000;
                break;
            case "NLJ":
                joinIO = leftTable.pageCount + (leftTable.pageCount * leftTable.tupleCount* 1.2); //1.2: Cost of find matching index tuples
                joinCost = (joinIO * QueryOptimizer.diskAccessTime)/1000;
                break;
            case "PNL":
                joinIO = leftTable.tupleCount + (leftTable.pageCount * rightTable.tupleCount);
                joinCost = (joinIO * QueryOptimizer.diskAccessTime)/1000;
                break;
            case "BNL":
                joinIO = leftTable.tupleCount + ((leftTable.pageCount)/QueryOptimizer.blockSize * rightTable.tupleCount);
                joinCost = (joinIO * QueryOptimizer.diskAccessTime)/1000;
                break;
        }
        return joinCost;
    }
    
    public static double calculateJoinFunction(IntializeTableAnkur leftTable, 
                                        IntializeTableAnkur rightTable, 
                                        String joinMethod){
        double joinCost = 0;
        double joinIO = 0;
        switch(joinMethod){
            case "TNL":
                joinIO = leftTable.pageCount + (leftTable.tupleCount * leftTable.pageCount * rightTable.pageCount);
                joinCost = (joinIO * QueryOptimizer.diskAccessTime)/1000;
                break;
            case "NLJ":
                joinIO = leftTable.pageCount + (leftTable.pageCount * leftTable.tupleCount* 1.2); //1.2: Cost of find matching index tuples
                joinCost = (joinIO * QueryOptimizer.diskAccessTime)/1000;
                break;
            case "PNL":
                joinIO = leftTable.tupleCount + (leftTable.pageCount * rightTable.tupleCount);
                joinCost = (joinIO * QueryOptimizer.diskAccessTime)/1000;
                break;
            case "BNL":
                joinIO = leftTable.tupleCount + ((leftTable.pageCount)/QueryOptimizer.blockSize * rightTable.tupleCount);
                joinCost = (joinIO * QueryOptimizer.diskAccessTime)/1000;
                break;
            case "SMJ":
                joinIO = 3 * (leftTable.pageCount + rightTable.pageCount);
                joinCost = (joinIO * QueryOptimizer.diskAccessTime)/1000;
                break;
            case "HJM":
                joinIO = 3 * (leftTable.pageCount + rightTable.pageCount);
                joinCost = (joinIO * QueryOptimizer.diskAccessTime)/1000;
                break;
            case "HJL":
                joinIO = 2 * 3 * (leftTable.pageCount + rightTable.pageCount);
                joinCost = (joinIO * QueryOptimizer.diskAccessTime)/1000;
                break;
                
        }
        return joinCost;
    }
}
