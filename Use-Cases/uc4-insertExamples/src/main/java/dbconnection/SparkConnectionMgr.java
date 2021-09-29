package dbconnection;

import static java.util.Collections.singletonList;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import com.redislabs.provider.redis.ReadWriteConfig;
import com.redislabs.provider.redis.RedisConfig;
import com.redislabs.provider.redis.RedisContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import scala.Tuple2;

public class SparkConnectionMgr {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SparkConnectionMgr.class);
	private static Map<String, DBCredentials> dbPorts = new HashMap<String, DBCredentials>();
	private static SparkSession session = null;

	private static class DBCredentials {

		protected String dbName;
		protected String url;
		protected String port;
		protected String userName;
		protected String userPwd;

		protected DBCredentials(String dbName, String url, String port, String userName, String userPwd) {
			this.dbName = dbName;
			this.url = url;
			this.port = port;
			this.userName = userName;
			this.userPwd = userPwd;
		}

	}

	static {
			dbPorts.put("mydb", new DBCredentials("mydb", "localhost", "3307", "root", "password"));
			dbPorts.put("mymongo", new DBCredentials("", "localhost", "27100", "", ""));
	}

		private static SparkSession getSession() {
		if (session == null) {
			session = SparkSession.builder().appName("Polystore").config("spark.master", "local")
					.config("spark.sql.shuffle.partitions", 5)
					.config("spark.some.config.option", "some-value")
					.config("spark.mongodb.input.uri", "mongodb://127.0.0.1:1/fakedb.fakecollection")
//			.config("spark.mongodb.output.uri", "mongodb://127.0.0.1:1/mymongo.productCollection")
					.getOrCreate();
			// session.sparkContext().setLogLevel("ERROR");
		}
		return session;
	}

	public static Dataset<Row> getSparkSessionForMongoDB(String dbName, String collectionName, String bsonQuery) {
		
		// https://docs.mongodb.com/manual/core/read-preference/#replica-set-read-preference-modes
		Map<String, String> readOverrides = new HashMap<String, String>();
		DBCredentials credentials = dbPorts.get(dbName);
		
		String mongoURL = "mongodb://" + credentials.url + ":" + credentials.port + "/" + dbName + "." + collectionName;
		getSession().sparkContext().conf().set("spark.mongodb.input.uri", mongoURL);
		getSession().sparkContext().conf().set("spark.mongodb.output.uri", mongoURL);
		
//		readOverrides.put("uri", "mongodb://" + credentials.url + ":" + credentials.port);
		readOverrides.put("database", dbName);
		readOverrides.put("collection", collectionName);
		readOverrides.put("readPreference.name", "primaryPreferred");
		
		JavaSparkContext jsc = new JavaSparkContext(getSession().sparkContext());
		
		ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

		Dataset<Row> res = (bsonQuery != null)
				? MongoSpark.load(jsc, readConfig).withPipeline(singletonList(Document.parse(bsonQuery))).toDF()
				: MongoSpark.load(jsc, readConfig).toDF();
		return res;

	}

	public static Dataset<Row> getDataset(String dbName, String physicalStructName) {
		DBCredentials credentials = dbPorts.get(dbName);
		DataFrameReader dfr = getSession().sqlContext().read().format("jdbc")
				.option("url", "jdbc:mysql://" + credentials.url + ":" + credentials.port + "/" + credentials.dbName)
				.option("user", credentials.userName).option("password", credentials.userPwd);

		Dataset<Row> d = dfr.option("dbtable", physicalStructName).load();
		return d;
	}

	public static void writeDataset(List<Row> rows, StructType structType, String formattype, String physicalStructName, String dbName){
		DBCredentials credentials = dbPorts.get(dbName);
		Dataset<Row> data = getSession().createDataFrame(rows, structType);
		String mongoURL = "mongodb://" + credentials.url + ":" + credentials.port + "/" + dbName + "." + physicalStructName;
		
		if(formattype.equals("mongo")){
			data.write()
                .format("mongo")
                .option("spark.mongodb.output.uri", mongoURL)
                .option("collection",physicalStructName)
                .mode(SaveMode.Append)
                .save();
		}else if(formattype.equals("jdbc"))
			{
			data.write()
				.format(formattype)
				.option("url", "jdbc:mysql://" + credentials.url + ":" + credentials.port + "/" + credentials.dbName)
				.option("dbtable",physicalStructName)
				.option("user", credentials.userName).option("password", credentials.userPwd)
				.mode(SaveMode.Append)
				.save();
			}
	}

	public static void writeKeyValue(String key, String value, String dbName){
		DBCredentials credentials = dbPorts.get(dbName);
		SparkConf sparkConf = new SparkConf()
                .setAppName("MyApp")
                .setMaster("local[*]")
                .set("spark.redis.host", credentials.url)
                .set("spark.redis.port", credentials.port);
        RedisConfig redisConfig = RedisConfig.fromSparkConf(sparkConf);
        ReadWriteConfig readWriteConfig = ReadWriteConfig.fromSparkConf(sparkConf);
		//        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(getSession().sparkContext());
        RedisContext redisContext = new RedisContext(jsc.sc());

		List<Tuple2<String, String>> data = Arrays.asList(new Tuple2<String, String>(key,value));
		RDD<Tuple2<String, String>> items = jsc.parallelize(data,1).rdd();
        redisContext.toRedisKV(items, 0, redisConfig, readWriteConfig);
	}

	public static void writeKeyValueHash(String key, List<Tuple2<String, String>> hash, String dbName){
		DBCredentials credentials = dbPorts.get(dbName);
		SparkConf sparkConf = new SparkConf()
                .setAppName("MyApp")
                .setMaster("local[*]")
                .set("spark.redis.host", credentials.url)
                .set("spark.redis.port", credentials.port);
        RedisConfig redisConfig = RedisConfig.fromSparkConf(sparkConf);
        ReadWriteConfig readWriteConfig = ReadWriteConfig.fromSparkConf(sparkConf);
		//        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(getSession().sparkContext());
        RedisContext redisContext = new RedisContext(jsc.sc());

		RDD<Tuple2<String, String>> items = jsc.parallelize(hash,1).rdd();
		redisContext.toRedisHASH(items, key,0, redisConfig, readWriteConfig);
	}

	public static Dataset<Row> getRowsFromKeyValue(String dbName, String keypattern) {
		DBCredentials credentials = dbPorts.get(dbName);
		SparkConf sparkConf = new SparkConf()
                .setAppName("MyApp")
                .setMaster("local[*]")
                .set("spark.redis.host", credentials.url)
                .set("spark.redis.port", credentials.port);
        RedisConfig redisConfig = RedisConfig.fromSparkConf(sparkConf);
        ReadWriteConfig readWriteConfig = ReadWriteConfig.fromSparkConf(sparkConf);
		//        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(getSession().sparkContext());
        RedisContext redisContext = new RedisContext(jsc.sc());

        // Get key value pairs non specific type of values.
        // Convert RDD to Dataset<Row>
        RDD<Tuple2<String, String>> rdd = redisContext.fromRedisKV(keypattern, 1, redisConfig, readWriteConfig);
        JavaRDD<Tuple2<String, String>> javaRDD = rdd.toJavaRDD();
        JavaRDD<Row> rowRDD = javaRDD.map((Function<Tuple2<String, String>, Row>) record -> {
            String key = record._1;
            String value = record._2;
            return RowFactory.create(key, value);
        });
		SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        StructType schema = DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("key",DataTypes.StringType,true), DataTypes.createStructField("value",DataTypes.StringType,true)));
        Dataset<Row> res = spark.createDataFrame(rowRDD, schema);
		return res;
	}

	public static Dataset<Row> getRowsFromKeyValueHashes(String dbName, String keypattern, StructType structTypeHash){
		DBCredentials credentials = dbPorts.get(dbName);
		SparkConf sparkConf = new SparkConf()
                .setAppName("MyApp")
                .setMaster("local[*]")
                .set("spark.redis.host", credentials.url)
                .set("spark.redis.port", credentials.port);
		SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
		Dataset<Row> res = spark.read().format("org.apache.spark.sql.redis")
                .option("keys.pattern",keypattern)
				.option("key.column", "id")
                .schema(structTypeHash).load();
		return res;
	} 

}
