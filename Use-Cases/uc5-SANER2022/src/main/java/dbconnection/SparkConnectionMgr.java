package dbconnection;

import static java.util.Collections.singletonList;

import java.util.*;

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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import scala.Tuple2;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import util.Dataset;
import util.Row;

public class SparkConnectionMgr {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SparkConnectionMgr.class);
	private static SparkSession session = null;

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
		Map<String, String> readOverrides = new HashMap<String, String>();
		DBCredentials credentials = DBCredentials.getDbPorts().get(dbName);
		MongoDatabase database = DBConnectionMgr.getMongoClient(credentials).getDatabase(dbName);
		MongoCollection<Document> collection = database.getCollection(collectionName);
		MongoCursor<Document> cursor = null;
		if (bsonQuery != null) {
			Document query = Document.parse(bsonQuery);
			cursor = collection.aggregate(Arrays.asList(query)).cursor();
		} else
			cursor = collection.find().cursor();

		Dataset<Row> res = new Dataset<Row>();
		while (cursor.hasNext()) {
			Document doc = cursor.next();
			res.add(new Row(doc));
		}
		cursor.close();
		return res;

	}

	public static Dataset<Row> getDataset(String dbName, String physicalStructName, String where) {
		Dataset<Row> res = new Dataset<Row>();

		DBCredentials credentials = DBCredentials.getDbPorts().get(dbName);
		DataFrameReader dfr = getSession().sqlContext().read().format("jdbc")
				.option("url", "jdbc:mysql://" + credentials.getUrl() + ":" + credentials.getPort() + "/" + credentials.getDbName())
				.option("user", credentials.getUserName()).option("password", credentials.getUserPwd());

		String query = "SELECT * FROM " + physicalStructName;
		if (where != null) {
			query += " WHERE " + where;
		}
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			conn = DriverManager.getConnection(
					"jdbc:mysql://" + credentials.getUrl() + ":" + credentials.getPort() + "/" + credentials.getDbName(),
					credentials.getUserName(), credentials.getUserPwd());
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			ResultSetMetaData rsmd = rs.getMetaData();
			while (rs.next()) {
				Map<String, Object> fieldValues = new HashMap<String, Object>();
				for (int i = 0; i < rsmd.getColumnCount(); i++) {
					String colName = rsmd.getColumnName(i + 1);
					Object value = rs.getObject(colName);
					fieldValues.put(colName, value);
				}

				res.add(new Row(fieldValues));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (rs != null)
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
		}

		return res;
	}

/**	public static void writeDataset(List<Row> rows, StructType structType, String formattype, String physicalStructName, String dbName){
		DBCredentials credentials = DBCredentials.getDbPorts().get(dbName);
		Dataset<Row> data = getSession().createDataFrame(rows, structType);
		String mongoURL = "mongodb://" + credentials.getUrl() + ":" + credentials.getPort() + "/" + dbName + "." + physicalStructName;
		
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
				.option("url", "jdbc:mysql://" + credentials.getUrl() + ":" + credentials.getPort() + "/" + credentials.getDbName())
				.option("dbtable",physicalStructName)
				.option("user", credentials.getUserName()).option("password", credentials.getUserPwd())
				.mode(SaveMode.Append)
				.save();
			}
	}
**/

	public static void writeKeyValue(String key, String value, String dbName){
		DBCredentials credentials = DBCredentials.getDbPorts().get(dbName);
		SparkConf sparkConf = new SparkConf()
                .setAppName("MyApp")
                .setMaster("local[*]")
                .set("spark.redis.host", credentials.getUrl())
                .set("spark.redis.port", String.valueOf(credentials.getPort()));
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
		DBCredentials credentials = DBCredentials.getDbPorts().get(dbName);
		SparkConf sparkConf = new SparkConf()
                .setAppName("MyApp")
                .setMaster("local[*]")
                .set("spark.redis.host", credentials.getUrl())
                .set("spark.redis.port", String.valueOf(credentials.getPort()));
        RedisConfig redisConfig = RedisConfig.fromSparkConf(sparkConf);
        ReadWriteConfig readWriteConfig = ReadWriteConfig.fromSparkConf(sparkConf);
		//        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(getSession().sparkContext());
        RedisContext redisContext = new RedisContext(jsc.sc());

		RDD<Tuple2<String, String>> items = jsc.parallelize(hash,1).rdd();
		redisContext.toRedisHASH(items, key,0, redisConfig, readWriteConfig);
	}

	public static Dataset<Row> getRowsFromKeyValue(String dbName, String keypattern) {
		DBCredentials credentials = DBCredentials.getDbPorts().get(dbName);
		Jedis jedis;
		jedis = new Jedis(credentials.getUrl(), credentials.getPort());
		ScanParams scanParams = new ScanParams().match(keypattern).count(100);
		String cur = ScanParams.SCAN_POINTER_START;
		boolean cycleIsFinished = false;
		List<String> keys = new ArrayList<String>();
		Dataset<Row> res = new Dataset<Row>();
		while(!cycleIsFinished) {
			ScanResult<String> scanResult =
					jedis.scan(cur, scanParams);
			keys.addAll(scanResult.getResult());

			cur = scanResult.getCursor();
			if (cur.equals("0")) {
				cycleIsFinished = true;
			}
		}
		if(keys.isEmpty())
			return null;
		List<String> values = jedis.mget(keys.toArray(new String[keys.size()]));

		for (int i = 0; i < keys.size(); i++) {
				String key = keys.get(i);
				Map<String, Object> map = new HashMap<String, Object>();
				String value = values.get(i);
				if(value != null) {
					map.put("key", key);
					map.put("value", value);
					res.add(new Row(map));
				}
		}

		jedis.close();
		return res;
	}

	public static Dataset<Row> getRowsFromKeyValueHashes(String dbName, String keypattern, StructType structTypeHash){
		DBCredentials credentials = DBCredentials.getDbPorts().get(dbName);
		Jedis jedis;
		jedis = new Jedis(credentials.getUrl(), credentials.getPort());
		ScanParams scanParams = new ScanParams().match(keypattern).count(100);
		String cur = ScanParams.SCAN_POINTER_START;
		boolean cycleIsFinished = false;
		Dataset<Row> res = new Dataset<Row>();
		while(!cycleIsFinished) {
			ScanResult<String> scanResult =
					jedis.scan(cur, scanParams);
			List<String> keysList = scanResult.getResult();

			boolean isStriped = (org.apache.commons.lang3.StringUtils.countMatches(keypattern, "*") == 1 && keypattern.endsWith("*"));
			String regex = keypattern.replaceAll("\\*","(.*)");
			java.util.regex.Pattern p = java.util.regex.Pattern.compile(regex);
			
			for (String key : keysList) {
				Map map = jedis.hgetAll(key);
				if(isStriped) {
					java.util.regex.Matcher matcher = p.matcher(key);
					matcher.find();
					key = matcher.group(1);
				}
				
				map.put("_id", key);
				res.add(new Row(map));
			}
			cur = scanResult.getCursor();
			if (cur.equals("0")) {
				cycleIsFinished = true;
			}
		}
		jedis.close();
		return res;
	}

	public static Dataset<Row> getRowsFromKeyValueList(String dbName, String keypattern, StructType structType){
		DBCredentials credentials = DBCredentials.getDbPorts().get(dbName);
		Dataset<Row> rows = new Dataset<>();
		Jedis jedis;
		jedis = new Jedis(credentials.getUrl(), credentials.getPort());
		ScanParams scanParams = new ScanParams().match(keypattern).count(100);
		String cur = ScanParams.SCAN_POINTER_START;
		boolean cycleIsFinished = false;
		Map<String, List<String>> listKV = new HashMap<>();
		while(!cycleIsFinished) {
			ScanResult<String> scanResult =
					jedis.scan(cur, scanParams);
			List<String> keysList = scanResult.getResult();

			//do whatever with the key-value pairs in result
			for (String key : keysList) {
				List<String> listvalues = jedis.lrange(key, 0, -1);
				listKV.put(key, listvalues);
			}
			cur = scanResult.getCursor();
			if (cur.equals("0")) {
				cycleIsFinished = true;
			}
		}
		String listFieldName = structType.fieldNames()[1];
		listKV.forEach((k,v) -> {
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("_id", k);
			map.put(listFieldName, v);
			rows.add(new Row(map));
		});
		jedis.close();
		return rows;
	} 
}
