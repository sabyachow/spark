package com.sparkTutorial.rdd.sumOfNumbers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SumOfNumbersProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
           print the sum of those numbers to console.

           Each row of the input file contains 10 prime numbers separated by spaces.
         */

        /*
        This was completely written by me : The Great Sabya !

         */

        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf().setAppName("prime_numbers").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> prime_nums = sc.textFile("in/prime_nums.text");
        JavaRDD<String> nums_str = prime_nums.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());

        JavaRDD<String> validNumbers = nums_str.filter(number -> !number.isEmpty());
        JavaRDD<Integer> nums = validNumbers.map(s -> new Integer(s));
        List <Integer> list = nums.take( 100);
        JavaRDD<Integer> integerRdd = sc.parallelize(list);
        Integer product = integerRdd.reduce((x, y) -> x + y);
        System.out.println("product is :" + product);
    }
}
