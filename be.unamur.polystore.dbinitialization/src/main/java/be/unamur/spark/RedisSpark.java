package be.unamur.app;

import com.redislabs.provider.redis.ReadWriteConfig;
import com.redislabs.provider.redis.RedisConfig;
import com.redislabs.provider.redis.RedisContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

public class RedisSpark {

    public static void main(String args[]){
//        SparkConf sparkConf = new SparkConf()
//                .setAppName("MyApp")
//                .setMaster("local[*]")
//                .set("spark.redis.host", "localhost")
//                .set("spark.redis.port", "6363");

        SparkSession spark = SparkSession
                .builder()
                .appName("MyApp")
                .master("local[*]")
                .config("spark.redis.host", "localhost")
                .config("spark.redis.port", "6363")
                .getOrCreate();

//        RedisConfig redisConfig = RedisConfig.fromSparkConf(sparkConf);
//        ReadWriteConfig readWriteConfig = ReadWriteConfig.fromSparkConf(sparkConf);
//
//        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
//        RedisContext redisContext = new RedisContext(jsc.sc());
//        RDD<Tuple2<String, String>> rdd = redisContext.fromRedisKV("PROFESSOR:6:NAME", 5, redisConfig, readWriteConfig);
//        StructType myStruct = new StructType();
//        Dataset<Row> dataset = spark.createDataFrame(rdd.toJavaRDD(), Tuple2.class);

        //comes from https://github.com/RedisLabs/spark-redis/blob/master/doc/dataframe.md#reading-redis-hashes
        Dataset<Row> dataset = spark.read().format("org.apache.spark.sql.redis")
                .option("keys.pattern","product*")
                .option("infer.schema", true).load();

        dataset.printSchema();
        dataset.show();

    }

}
