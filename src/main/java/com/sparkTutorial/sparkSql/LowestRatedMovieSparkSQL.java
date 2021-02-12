package com.sparkTutorial.sparkSql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LowestRatedMovieSparkSQL {
    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("LowestRatedMovieSpark").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession session = SparkSession.builder()
                .appName("LowestRatedMovieSpark")
                .master("local[1]")
                .getOrCreate();

        JavaRDD<Ratings> movies = sc.textFile("in/u.data")
                .map(line -> {
                    String[] parts = line.split("\t");
                    Ratings rating = new Ratings();
                    rating.setuId(Integer.parseInt(parts[0].trim()));
                    rating.setMovieId(Integer.parseInt(parts[1].trim()));
                    rating.setRating(Integer.parseInt(parts[2].trim()));
                    return rating;
                });

        Dataset<Row> moviesDF = session.createDataFrame(movies, Ratings.class);
        moviesDF.createOrReplaceTempView("rating");

        Dataset<Row> teenagersDF = session.sql("SELECT count(*) FROM rating where rating = 1");
        teenagersDF.show();
    }
}
