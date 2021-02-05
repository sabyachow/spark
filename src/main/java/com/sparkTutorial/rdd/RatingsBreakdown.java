package com.sparkTutorial.rdd;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;

public class RatingsBreakdown {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("RatingsBreakDown").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/u.data");
        JavaRDD<String> movies = lines.filter(line -> Float.valueOf(line.split("\t")[2]) > 4);

        System.out.println("Number of Movie Rated 5: "+movies.count());
        //We will have to come back ehre to list down all the Rating and their Counts
    }
}
