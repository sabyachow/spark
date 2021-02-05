package com.sparkTutorial.sparkSql;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.col;


public class MySparkSQLExample {
    public static void main(String[] args) throws Exception {
        SparkSession session = SparkSession.builder()
                .appName("HousePriceSolution")
                .master("local[1]")
                .getOrCreate();

        Dataset<Row> realEstate = session.read().option("header", "true").csv("in/RealEstate.csv");
        //realEstate.show();
        //realEstate.printSchema();
        //realEstate.select(col("Location"), col("Price"), col("Bedrooms")).show();
        //realEstate.filter(col("Bedrooms").gt(5)).show();
        realEstate.createOrReplaceTempView("RealEstate");
        Dataset<Row> sqlDF = session.sql("SELECT * FROM RealEstate WHERE BEDROOMS = 5 " +
                "AND PRICE < 500000");
        sqlDF.show();

    }
}
