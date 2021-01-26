package be.unamur.spark;

import com.redislabs.provider.redis.ReadWriteConfig;
import com.redislabs.provider.redis.RedisConfig;
import com.redislabs.provider.redis.RedisContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.Arrays;


public class RedisSpark {

    public static void main(String args[]){
        SparkConf sparkConf = new SparkConf()
                .setAppName("MyApp")
                .setMaster("local[*]")
                .set("spark.redis.host", "localhost")
                .set("spark.redis.port", "6363");
        RedisConfig redisConfig = RedisConfig.fromSparkConf(sparkConf);
        ReadWriteConfig readWriteConfig = ReadWriteConfig.fromSparkConf(sparkConf);
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        RedisContext redisContext = new RedisContext(jsc.sc());

        // Get key value pairs non specific type of values.
        // Convert RDD to Dataset<Row>
        RDD<Tuple2<String, String>> rdd = redisContext.fromRedisKV("PROFESSOR:*:NAME", 5, redisConfig, readWriteConfig);
        JavaRDD<Tuple2<String, String>> javaRDD = rdd.toJavaRDD();
        JavaRDD<Row> rowRDD = javaRDD.map((Function<Tuple2<String, String>, Row>) record -> {
            String key = record._1;
            String value = record._2;
            return RowFactory.create(key, value);
        });
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        StructType schema = DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("key",DataTypes.StringType,true), DataTypes.createStructField("value",DataTypes.StringType,true)));
        Dataset<Row> res = spark.createDataFrame(rowRDD, schema);
        res.printSchema();
        res.show();

        //Retrieves Dataset<Row> directly, only works on Redis Hashes structures as values
//        comes from https://github.com/RedisLabs/spark-redis/blob/master/doc/dataframe.md#reading-redis-hashes
//        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        Dataset<Row> dataset = spark.read().format("org.apache.spark.sql.redis")
                .option("keys.pattern","product*")
                .option("key.column","id")
                .option("infer.schema", true).load();

        dataset.printSchema();
        dataset.show();

    }

}
