package be.unamur.spark;

import java.util.Date;
import java.util.Iterator;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.*;

public class TestJDBCSpark {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .config("spark.some.config.option", "some-value").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        System.out.println(new Date());
        DataFrameReader dfr = spark.sqlContext()
                .read()
                .format("jdbc")
                .option("url", "jdbc:mysql://127.0.0.1:3307/mydb")
                .option("user", "root")
                .option("password", "mysql");
        System.out.println("before t1: " + new Date());
        Dataset<Row> d1 = dfr.option("dbtable", "User").load();
        d1 = d1.where("id = technicaluserid124");
        d1.show();
        System.out.println("before t2: " + new Date());
        Dataset<Row> d2 = dfr.option("dbtable", "CreditCard").load();
        d2 = d2.where("id = technicaluserid124");
        Iterator<Row> it = d2.toLocalIterator();

        if (it.hasNext()) {
// Object r = it.next().getAs("NCLI");
            System.out.println(it.next());
        }
        d2.show();
        System.out.println("before join:" + new Date());
        Dataset<Row> d = d1.join(d2, d1.col("id").equalTo(d2.col("userid")));
        System.out.println("before printing:" + new Date());
        d.show();

        System.out.println("-----------------------:" + new Date());

    }


}