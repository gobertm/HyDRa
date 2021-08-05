package be.unamur.spark;

import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.Date;

public class InsertSpark {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic insert example")
                .config("spark.master", "local")
                .config("spark.some.config.option", "some-value").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        System.out.println(new Date());
        DataFrameReader dfr = spark.sqlContext()
                .read()
                .format("jdbc")
                .option("url", "jdbc:mysql://127.0.0.1:3307/mydb")
                .option("user", "root")
                .option("password", "password");

        Dataset<Row> d = dfr.option("dbtable", "directorTable").load();
        d.show();
        Dataset<Row> toWriteDF = d.withColumn("id", "FAKEID").withColumn("fullname", "FAKEFULLNAME");
        toWriteDF.show();
        toWriteDF.write()
                .format("jdbc")
                .option("url", "jdbc:mysql://127.0.0.1:3307/mydb")
                .option("user", "root")
                .option("password", "password")
                .option("dbtable","directorTable")
                .mode(SaveMode.Append)
                .save();
        spark.createDataFrame(Arrays.asList())

}
