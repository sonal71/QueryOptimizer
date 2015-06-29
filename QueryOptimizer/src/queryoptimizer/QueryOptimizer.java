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
        Double totalCost=0.0;
        
        Map<String, InitializeTable> tablesInfo = new HashMap<>();
        File file = new File("src/queryoptimizer/inputAnkur.txt");
        Scanner input = new Scanner(file);
        String line = input.nextLine(), currentQuery="";
        while(input.hasNextLine() && !line.equals("###")) {
            String[] tableInfo = line.split(" ");
            tablesInfo.put(tableInfo[0], new InitializeTable(tableInfo[0], Double.parseDouble(tableInfo[1]), Double.parseDouble(tableInfo[2])));
            line = input.nextLine();
        }
        Map<Integer, Map<String, Double>> joinCost = new HashMap<>();
        Map<Integer, Map<String, Double>> projectionCost = new HashMap<>();
        Map<Integer, Map<String, Double>> groupByCost = new HashMap<>();
        
        Map<String, ArrayList<Object>> currentPlan = new HashMap<>();
        
        Map<String, Double> totalCostQuery = new HashMap<>();
        
        Double selectivity;
        int co_flag,i=0;
        String[] params;
        while(input.hasNextLine()) {
            line = input.nextLine();
            params = line.split(" ");
            switch(params[0]) {
                case "Q": // Starting of new query
                    currentQuery = params[1];
                    System.out.println("---------------"+currentQuery+"---------------");
                    break;
                case "J": // Joins
                    Map<String, Double> tempMap = new HashMap<>();
                    co_flag = Integer.parseInt(params[4]);
                    selectivity = Double.parseDouble(params[5]);
                    if(co_flag == 0) {
                        for(String joinMethod: joinMethods) {
                            tempMap.put(tablesInfo.get(params[1]).tableName+" join "+tablesInfo.get(params[2]).tableName+" "+joinMethod, 
                                    calculateJoinFunction(tablesInfo.get(params[1]), tablesInfo.get(params[2]), joinMethod));
                            joinCost.put(i, tempMap);
                            tempMap.put(tablesInfo.get(params[2]).tableName+" join "+tablesInfo.get(params[1]).tableName+" "+joinMethod, 
                                    calculateJoinFunction(tablesInfo.get(params[2]), tablesInfo.get(params[1]), joinMethod));
                            joinCost.put(i, tempMap);
                        }
                        i++;
                    } else {
                        for(String joinMethod: correlationJoinMethods) {
                            tempMap.put(tablesInfo.get(params[1]).tableName+" join "+tablesInfo.get(params[2]).tableName+" "+joinMethod, 
                                    calculateJoinFunction(tablesInfo.get(params[1]), tablesInfo.get(params[2]), joinMethod));
                            joinCost.put(i, tempMap);
                            tempMap.put(tablesInfo.get(params[2]).tableName+" join "+tablesInfo.get(params[1]).tableName+" "+joinMethod, 
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
                    Map<String, Double> tempProjectMap = new HashMap<>();
                    tempProjectMap.put("Project "+params[1], calculateProjectFunction(tablesInfo.get(params[1])));
                    projectionCost.put(0, tempProjectMap);
                    break;
                case "G": //Group By
                    Map<String, Double> tempGroupByMapSorted = new HashMap<>();
                    tempGroupByMapSorted.put("GroupBy on "+params[1], calculateGroupByFunction(tablesInfo.get(params[1]), "SMJ"));
                    groupByCost.put(0, tempGroupByMapSorted);
                    Map<String, Double> tempGroupByMap = new HashMap<>();
                    tempGroupByMap.put("GroupBy on "+params[1], calculateGroupByFunction(tablesInfo.get(params[1]), ""));
                    groupByCost.put(1, tempGroupByMap);
                    break;
                case "##": //To indicate ending of query
                    ArrayList<Object> tempArrayList = new ArrayList<>();
                    for(int j=0;j<i;j++) {    
                        tempArrayList.add(((HashMap<Integer, Map<String, Double>>) joinCost).clone());
                        Double bestJoin = Collections.min(joinCost.get(j).values());
                        Set<String> joinKeys = joinCost.get(j).keySet();
                        for(String key: joinKeys) {
                            if(Objects.equals(joinCost.get(j).get(key), bestJoin)){
                                System.out.println(key+" :"+joinCost.get(j).get(key));
                                totalCost += Collections.min(joinCost.get(j).values());
                                break;
                            }
                        }
                    }
                    for(i=0;i<1;i++){
                        tempArrayList.add(((HashMap<Integer, Map<String, Double>>) projectionCost).clone());
                        Set<String> projectionKeys = projectionCost.get(i).keySet();
                        for(String key: projectionKeys) {
                            System.out.println(projectionCost.get(i));
                            totalCost += projectionCost.get(i).get(key);
                        }
                    }
                    for(i=0;i<1;i++){
                        tempArrayList.add(((HashMap<Integer, Map<String, Double>>) groupByCost).clone());
                        Set<String> groupByKeys = groupByCost.get(i).keySet();
                        for(String key: groupByKeys) {
                            System.out.println(groupByCost.get(i));
                            totalCost += groupByCost.get(i).get(key);
                        }
                    }
                    currentPlan.put(currentQuery, tempArrayList);
                    joinCost.clear();
                    projectionCost.clear();
                    groupByCost.clear();
                    System.out.println("# of Disk I/O's :"+Math.round(totalCost/0.012));
                    System.out.println("Processing time (Hr:mm:ss) :"+Math.round(totalCost/3600)+":"+Math.round((totalCost%3600)/60)+":"+Math.round((totalCost%60)));
                    totalCostQuery.put(currentQuery, totalCost);
                    totalCost=0.0;
                    i=0;
                    break;
                default:
                    break;
            }
        }
        Set<String> currentPlanKey = currentPlan.keySet();
        for(String key: currentPlanKey) {
            System.out.println(key+" :"+currentPlan.get(key));
        }
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
    public static double calculateProjectFunction(InitializeTable table) {
        double projectCost = 0;
        double projectIO = 0;
        projectIO = Math.log10(table.pageCount) +3 *table.pageCount;
        projectCost = (projectIO * QueryOptimizer.diskAccessTime)/1000;
        return projectCost;
    }
    public static double calculateGroupByFunction(InitializeTable table, String joinMethod) {
        double groupByCost = 0;
        double groupByIO = 0;
        if(joinMethod.equals("SMJ")){
            groupByIO = 2 * table.pageCount;
            groupByCost = (groupByIO * QueryOptimizer.diskAccessTime)/1000;
        } else {
            groupByIO = 2 * table.pageCount + (table.pageCount * Math.log10(table.pageCount));
            groupByCost = (groupByIO * QueryOptimizer.diskAccessTime)/1000;
        }
        return groupByCost;
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