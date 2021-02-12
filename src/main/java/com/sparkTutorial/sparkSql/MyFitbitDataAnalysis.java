package com.sparkTutorial.sparkSql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/*
Ok, now I do not know when I will revisti this code again, hence leaving some notes so that I can pick up.
In this program I am trying to load a JSON file I got from my fitbit account on the
distance I walked every day for one year, not spark can not read a normal JSON file and it is a known problem.
What is recomends is to convert a JSON file to a JSONL file. Which I did not do here. So what did I do?

I read the JSON file using the simple JSON library and got a JSONObject array, so I used the
array in memory to get a List adn then used that list of JSONObjects to create and RDD.
Then used the RDD map fucntion to transfor it to an RDD of Dictance object.

Used that RDD to create a Data Set so that i can querry the DataSet.
 */
import java.io.FileReader;

public class MyFitbitDataAnalysis {
    public static void main(String[] args) throws Exception {Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("FitbitDataAnalysis").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession session = SparkSession.builder()
                .appName("FitbitDataAnalysis")
                .master("local[1]")
                .getOrCreate();

        //Read the JSON file using the simple JSON Parser.
        Object obj = new org.json.simple.parser.JSONParser().parse(new FileReader("in/distance_1613014152406.json"));
        JSONObject jo = (JSONObject)obj;

        //Get the array of data from the JSON file
        JSONArray ja = (JSONArray) jo.get("activities-distance");

        //you can iterate through the JSON Array to see its content.
        //Iterator itr1 = ja.iterator();
        //while (itr1.hasNext()){
        //    System.out.println(itr1.next());
        //}
        //System.out.println(jo.toJSONString());

        //Conver the JSON Array to a List
        java.util.List<JSONObject> distanceList = ja.subList(0, ja.size()-1);

        //Used that list to create and RDD
        JavaRDD<JSONObject> distanceJavaRDD = sc.parallelize(distanceList);

        JavaRDD<Distance> responseRDD = distanceJavaRDD
                .map(jsonObject -> {
                    Distance distance = new Distance();
                    distance.setDateTime(jsonObject.get("dateTime").toString());
                    distance.setDistance(Double.parseDouble(jsonObject.get("value").toString()));
                    return distance;
                });

        Dataset<Row> distanceCovered = session.createDataFrame(responseRDD, Distance.class);

        distanceCovered.show(); //shows the data in tabular format
        distanceCovered.printSchema(); //shows the schema
        distanceCovered.createOrReplaceTempView("distance");

        //now time to Query

        Dataset<Row> fiveK = session.sql("SELECT * FROM distance where distance > 3");
        fiveK.show();

        Dataset<Row> fiveKcount = session.sql("SELECT count(*) FROM distance where distance > 3");
        fiveKcount.show();

    }

}
