package com.sparkTutorial.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.Map;


public class MyRDDExamples {
    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);


        SparkConf conf = new SparkConf().setAppName("MyRDD").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("in/u.data");

        /*for(String line:lines.collect()){
            System.out.println("Sabyasachi Spark Output (Basic RDD):"+line);
            //System.out.println("Sabyasachi Spark Output (Basic RDD):"+line.split("\t")[2]);
        }*/


        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        for(Integer len:lineLengths.collect()){
            System.out.println("Sabyasachi Spark Output (Basic RDD Length):"+len.toString());
        }


        int totalLength = lineLengths.reduce((a, b) -> a + b);
        System.out.println("Sabyasachi Spark Output:"+totalLength);

        JavaPairRDD<String, Integer> pairs = lines.mapToPair(line -> new Tuple2(line.split("\t")[2],1));
        System.out.println("Sabyasachi Spark Count (JavaPairRDD RDD):"+pairs.count());

        /*for(scala.Tuple2<String, Integer> tuple: pairs.collect()){
            System.out.println("Sabyasachi Spark Output (MAP RDD):"+tuple._1()+":"+tuple._2());
        }*/

        JavaPairRDD<String, Integer> counts = (pairs.reduceByKey((a, b) -> a + b)).sortByKey(true);

        for(Map.Entry<String, Integer> ratings: counts.collectAsMap().entrySet()){
           System.out.println("Sabyasachi Spark Output (REDUCE RDD):"+ratings.getKey()+":"+ratings.getValue());
        }
    }
}


