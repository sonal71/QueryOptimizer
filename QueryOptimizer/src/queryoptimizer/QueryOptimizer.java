package queryoptimizer;

import java.util.*;

public class QueryOptimizer {
    public static double pageSize = 4000; //Bytes 
    public static double blockSize = 100; //Pages
    public static double diskAccessTime = 12; //ms
    
    
    public static void main(String[] args) {
        String[] joinMethods = new String[]{"NLJ"
                //, "SNL", "BNL", "TNL", "SMJ", "HJM", "HJL"
        };
        ArrayList<IntializeTableParams> tablesInfo = new ArrayList<>();
        tablesInfo.add(new IntializeTableParams("T1", 20, 1000));
        tablesInfo.add(new IntializeTableParams("T2", 20, 1000));
        tablesInfo.add(new IntializeTableParams("T3", 100, 2000));
        
        Map<String, Double> firstJoin = new HashMap<>();
        Map<String, Double> secondJoin = new HashMap<>();
        int i=0;
        for(IntializeTableParams firstTable: tablesInfo){
            for(IntializeTableParams secondTable: tablesInfo){
                if(firstTable != secondTable){
                    System.out.println(firstTable);
                    System.out.println(secondTable);
                    for(String joinMethod: joinMethods){
                        firstJoin.put(joinMethod, calculateJoinFunction(firstTable,secondTable, joinMethod));
                    }
                    for(String joinMethod: joinMethods){
                        firstJoin.put(joinMethod, calculateJoinFunction(secondTable,firstTable, joinMethod));
                    }
                    for(IntializeTableParams thirdTable: tablesInfo){
                        if(firstTable != thirdTable && secondTable != thirdTable){
                            System.out.println(thirdTable);
                            System.out.println("++++++++++++++++++++++++++++++++++++");
                            for(String joinMethod: joinMethods){
                                secondJoin.put(joinMethod, calculateJoinFunction(firstTable,thirdTable, joinMethod));
                            }
                            for(String joinMethod: joinMethods){
                                secondJoin.put(joinMethod, calculateJoinFunction(thirdTable,firstTable, joinMethod));
                            }
                        }
                    }    
                }
            }    
        }
        System.out.println(i);
        System.out.println(firstJoin);
        System.out.println(Collections.max(firstJoin.values()));
        System.out.println(secondJoin);
        System.out.println(Collections.max(secondJoin.values()));
    }
    
    public static double calculateJoinFunction(IntializeTableParams leftTable, 
                                        IntializeTableParams rigntTable, 
                                        String joinMethod){
        double joinCost = 0;
        double joinIO = 0;
        switch(joinMethod){
            case "SNL":
                joinIO = leftTable.getPageCount() + (leftTable.getTuplesCount() * leftTable.getPageCount() * rigntTable.getPageCount());
                joinCost = (joinIO * QueryOptimizer.diskAccessTime)/1000;
                break;
            case "NLJ":
                joinIO = leftTable.getPageCount() + (leftTable.getPageCount() * leftTable.getTuplesCount()* 1.2); //1.2: Cost of find matching index tuples
                joinCost = (joinIO * QueryOptimizer.diskAccessTime)/1000;
                break;
            case "BNL":
                break;
            case "SMJ":
                break;
            case "HJM":
                break;
            case "TNL":
                break;
            case "HJL":
                break;
                
        }
        return joinCost;
    }
}

class IntializeTableParams {
    String tableName;
    double tupleCount;
    double pageCount;
    
    public IntializeTableParams(String tableName, double tupleSize, double pageCount){
        this.tableName = tableName;
        this.tupleCount = QueryOptimizer.pageSize/tupleSize;
        this.pageCount = pageCount;
    }
    public String getTableName () {
        return tableName;
    }
    public double getTuplesCount () {
        return tupleCount;
    }
    public double getPageCount () {
        return pageCount;
    }
    @Override
    public String toString () {
        return "Table name: "+this.tableName+" Tuple count: "+this.tupleCount+" Page size: "+this.pageCount;
    }
}
