package com.sparkTutorial.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Map;

public class LowestRatedMovieSpark {
    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("LowestRatedMovieSpark").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> movies = sc.textFile("in/u.item");

        JavaPairRDD<String, String> moviesPair =
                movies.mapToPair(line -> new Tuple2(line.split("\\|")[0], line.split("\\|")[1]));
        System.out.println("Sabyasachi Spark Count (JavaPairRDD RDD):"+movies.count());

        //for(scala.Tuple2<String, Integer> tuple: moviesPair.collect()){
            //System.out.println("Sabyasachi Spark Ratings: "+tuple._1()+":"+tuple._2());
        //}

        Map<String, String> movieNamesMap = moviesPair.collectAsMap();

        JavaRDD<String> ratings = sc.textFile("in/u.data");
        JavaPairRDD<String, Integer> ratingPairs =
                ratings.mapToPair(line -> new Tuple2(line.split("\t")[2], line.split("\t")[1])).sortByKey(true);

        for(scala.Tuple2<String, Integer> tuple: ratingPairs.collect()){
            System.out.println("Sabyasachi Spark Movie Ratings: "+movieNamesMap.get(tuple._2())+":"+tuple._1());
        }

    }
}
