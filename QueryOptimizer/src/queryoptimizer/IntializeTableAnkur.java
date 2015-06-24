package queryoptimizer;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author ankurpandit
 */
public class IntializeTableAnkur {
    String tableName;
    double tupleCount;
    double pageCount;
    double tupleSize;
    
    public IntializeTableAnkur(String tableName, double tupleSize, double pageCount){
        this.tableName = tableName;
        this.pageCount = pageCount;
        this.tupleSize = tupleSize;
        this.tupleCount = pageCount*(QueryOptimizer.pageSize/tupleSize);
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
    public double getTupleSize () {
        return tupleSize;
    }
    @Override
    public String toString () {
        return "Table name: "+this.tableName+" Tuple count: "+this.tupleCount+" Page size: "+this.pageCount;
    }
}