package queryoptimizer;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class QueryOptimizer {
    public static double pageSize = 4000; //Bytes 
    public static double blockSize = 100; //Pages
    public static double diskAccessTime = 12; //ms
    
    public static void main(String[] args) throws FileNotFoundException {
        String[] joinMethods = new String[]{"TNL", "PNL", "BNL", "SMJ", "HJM", "HJL"};
        String[] correlationJoinMethods = new String[]{"TNL", "PNL", "BNL"};
        
        Map<String, InitializeTable> tablesInfo = new HashMap<>();
        File file = new File("src/queryoptimizer/inputAnkur.txt");
        Scanner input = new Scanner(file);
        String line = input.nextLine();
        while(input.hasNextLine() && !line.equals("###")) {
            String[] tableInfo = line.split(" ");
            tablesInfo.put(tableInfo[0], new InitializeTable(tableInfo[0], Double.parseDouble(tableInfo[1]), Double.parseDouble(tableInfo[2])));
            line = input.nextLine();
        }
        
        Map<Integer, Map<String, Double>> joinCost = new HashMap<>();
        Map<String, Double> projectionCost = new HashMap<>();
        Map<String, Double> groupByCost = new HashMap<>();
        
        Double selectivity;
        int co_flag,i=0;
        String[] params;
        while(input.hasNextLine()) {
            line = input.nextLine();
            params = line.split(" ");
            switch(params[0]) {
                case "Q": // Starting of new query
                    String currentQuery = params[1];
                    System.out.println("---------------"+currentQuery+"---------------");
                    break;
                case "J": // Joins
                    Map<String, Double> tempMap = new HashMap<>();
                    co_flag = Integer.parseInt(params[4]);
                    selectivity = Double.parseDouble(params[5]);
                    if(co_flag == 1) {
                        for(String joinMethod: joinMethods) {
                            tempMap.put(tablesInfo.get(params[1]).tableName+" join "+tablesInfo.get(params[2]).tableName+". Join Method used: "+joinMethod, 
                                    calculateJoinFunction(tablesInfo.get(params[1]), tablesInfo.get(params[2]), joinMethod));
                            joinCost.put(i, tempMap);
                            tempMap.put(tablesInfo.get(params[2]).tableName+" join "+tablesInfo.get(params[1]).tableName+". Join Method used: "+joinMethod, 
                                    calculateJoinFunction(tablesInfo.get(params[2]), tablesInfo.get(params[1]), joinMethod));
                            joinCost.put(i, tempMap);
                        }
                        i++;
                    } else {
                        for(String joinMethod: correlationJoinMethods) {
                            tempMap.put(tablesInfo.get(params[1]).tableName+" join "+tablesInfo.get(params[2]).tableName+". Join Method used: "+joinMethod, 
                                    calculateJoinFunction(tablesInfo.get(params[1]), tablesInfo.get(params[2]), joinMethod));
                            joinCost.put(i, tempMap);
                            tempMap.put(tablesInfo.get(params[2]).tableName+" join "+tablesInfo.get(params[1]).tableName+". Join Method used: "+joinMethod, 
                                    calculateJoinFunction(tablesInfo.get(params[2]), tablesInfo.get(params[1]), joinMethod));
                            joinCost.put(i, tempMap);
                        }
                        i++;
                    }
                    tablesInfo.put(params[3], new InitializeTable(params[3], 
                                        tablesInfo.get(params[1]).tupleSize+tablesInfo.get(params[2]).tupleSize, 
                                        selectivity*tablesInfo.get(params[1]).pageCount*tablesInfo.get(params[2]).pageCount));
                    break;
                case "P": // Projection
                    System.out.println("Project "+params[1]);
                    break;
                case "G": //Group By
                    System.out.println("Group by "+params[1]);
                    break;
                case "##": //To indicate ending of query
                    for(int j=0;j<i;j++) {    
                        Double bestJoin = Collections.min(joinCost.get(j).values());
                        Set<String> keys = joinCost.get(j).keySet();
                        for(String key: keys) {
                            if(Objects.equals(joinCost.get(j).get(key), bestJoin)){
                                System.out.println(key+" :"+joinCost.get(j).get(key));
                                break;
                            }
                        }
                    }
                    joinCost.clear();
                    projectionCost.clear();
                    groupByCost.clear();
                    i=0;
                    break;
                default:
                    break;
            }
        }
        System.out.println(tablesInfo);
                
    }
    
    public static double calculateJoinFunction(InitializeTable leftTable, 
                                        InitializeTable rightTable, 
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
class InitializeTable {
    String tableName;
    double tupleCount;
    double pageCount;
    double tupleSize;
    
    public InitializeTable(String tableName, double tupleSize, double pageCount){
        this.tableName = tableName;
        this.pageCount = pageCount;
        this.tupleSize = tupleSize;
        this.tupleCount = pageCount*(QueryOptimizer.pageSize/tupleSize);
    }
    @Override
    public String toString () {
        return "Table name: "+this.tableName+" Tuple count: "+this.tupleCount+" Page size: "+this.pageCount;
    }
    
}