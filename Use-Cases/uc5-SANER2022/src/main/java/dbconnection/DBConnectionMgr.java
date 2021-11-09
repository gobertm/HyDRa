package dbconnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import org.bson.Document;
import org.bson.conversions.Bson;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;

public class DBConnectionMgr {

	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DBConnectionMgr.class);
	private static Map<String, DBConnection> mapDB = new HashMap<String, DBConnection>(); 
	private static Map<String, MongoClient> mapMongoConnection = new HashMap<>();
	private static MongoClient mongoClient;
	private static Connection connection;
	
	public static Map<String, DBConnection> getMapDB(){
		return mapDB;
	}

	public static void update(Bson filter, Bson updateOp, String struct, String db){
		DBCredentials credentials = DBCredentials.getDbPorts().get(db);
		if(credentials.getDbType().equals("mongodb")){ 
			MongoDatabase mongoDatabase = getMongoClient(credentials).getDatabase(db);
	        MongoCollection<Document> structCollection = mongoDatabase.getCollection(struct);
	        structCollection.updateMany(filter, updateOp);
			logger.info("Update many on collection [{}], filter [{}]",db+ " - "+struct, filter);
		}
		else{
			logger.error("Can't perform update, wrong database type, need document db. Database : '{}' , type : '{}' ",credentials.getDbName(), credentials.getDbType());
			throw new RuntimeException("Can't perform update, wrong database type, need document db");
		}
	}

	public static void upsertMany(Bson filter, Bson updateOp, String struct, String db){
		DBCredentials credentials = DBCredentials.getDbPorts().get(db);
		if(credentials.getDbType().equals("mongodb")){ 
			MongoDatabase mongoDatabase = DBConnectionMgr.getMongoClient(credentials).getDatabase(db);
	        MongoCollection<Document> structCollection = mongoDatabase.getCollection(struct);
			UpdateOptions options = new UpdateOptions().upsert(true);
	        structCollection.updateMany(filter, updateOp, options);
			logger.info("Update many on collection [{}], filter [{}]",db+ " - "+struct, filter);
		}
		else{
			logger.error("Can't perform update, wrong database type, need document db. Database : '{}' , type : '{}' ",credentials.getDbName(), credentials.getDbType());
			throw new RuntimeException("Can't perform update, wrong database type, need document db");
		}
	}

	public static void upsertMany(Bson filter, Bson updateOp, List<Bson> arrayFiltersConditions, String struct, String db){
		DBCredentials credentials = DBCredentials.getDbPorts().get(db);
		if(credentials.getDbType().equals("mongodb")){ 
			MongoDatabase mongoDatabase = DBConnectionMgr.getMongoClient(credentials).getDatabase(db);
	        MongoCollection<Document> structCollection = mongoDatabase.getCollection(struct);
			UpdateOptions updateOptions = new UpdateOptions().arrayFilters(arrayFiltersConditions);
	        structCollection.updateMany(filter, updateOp, updateOptions);
			logger.info("Update many on collection [{}], filter [{}]",db+ " - "+struct, filter);
		}
		else{
			logger.error("Can't perform update, wrong database type, need document db. Database : '{}' , type : '{}' ",credentials.getDbName(), credentials.getDbType());
			throw new RuntimeException("Can't perform update, wrong database type, need document db");
		}
	}


	public static void insertMany(List<Document> documents, String struct, String db) {
		DBCredentials credentials = DBCredentials.getDbPorts().get(db);
		if(!credentials.getDbType().equals("mongodb")){
			logger.error("Can't perform update, wrong database type, need document db. Database : '{}' , type : '{}' ",credentials.getDbName(), credentials.getDbType());
			throw new RuntimeException("Can't perform update, wrong database type, need document db");
		}
		else{
			MongoDatabase mongoDatabase = getMongoClient(credentials).getDatabase(db);
			MongoCollection<Document> structCollection = mongoDatabase.getCollection(struct);
			structCollection.insertMany(documents);
			logger.info("Insert many on collection [{}],  [{}] documents",db+ " - "+struct, documents.size());
		}
	}

	public static MongoClient getMongoClient(DBCredentials credentials) {
		mongoClient = null;
		mongoClient = mapMongoConnection.get(credentials.getUrl()+credentials.getPort());
		if (mongoClient == null) {
			mongoClient = MongoClients.create(
					MongoClientSettings.builder()
							.applyToClusterSettings(builder ->
									builder.hosts(Arrays.asList(new ServerAddress(credentials.getUrl(), credentials.getPort()))))
							.build());
			mapMongoConnection.put(credentials.getUrl()+credentials.getPort(), mongoClient);
		}
		return mongoClient;
	}

	public static void insertInTable(List<String> columns, List<List<Object>> rows, String struct, String db) {
		DBCredentials credentials = DBCredentials.getDbPorts().get(db);
		if((credentials.getDbType().equals("mariadb") || credentials.getDbType().equals("sqlite"))){
			try {
				String sql = "INSERT INTO " + struct + "("+String.join(",",columns)+") VALUES ("+String.join(",", Collections.nCopies(columns.size(),"?"))+")";
				PreparedStatement statement = getJDBCConnection(credentials).prepareStatement(sql);
				for (List<Object> row : rows) {
					for (int i = 1; i <= row.size(); i++) {
						statement.setObject(i,row.get(i-1));
					}
					statement.addBatch();
					statement.clearParameters();
				}
				int[] result = statement.executeBatch();
				logger.info("BATCH INSERT INTO '{}' - '{}' lines ", struct, result.length);  
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error("SQL Error in preparedStatement");
			}
		}
		else{
			logger.error("Can't perform update, wrong database type, need relational (mariadb or sqlite) database. Database : '{}' , type : '{}' ",credentials.getDbName(), credentials.getDbType());
			throw new RuntimeException("Can't perform update, wrong database type, need document db");
		}
	}


	public static void updateInTable(String filtercolumn, String filtervalue, List<String> columns, List<Object> values, String struct, String db) {
		DBCredentials credentials = DBCredentials.getDbPorts().get(db);
		if((credentials.getDbType().equals("mariadb") || credentials.getDbType().equals("sqlite"))){
			try {
				StringJoiner joiner = new StringJoiner(",", "", "= ?");
				for (String s : columns) {
					joiner.add(s);
				}
				String sql = "UPDATE " + struct + " SET " + joiner +" WHERE "+filtercolumn +"= ?";
				PreparedStatement statement = getJDBCConnection(credentials).prepareStatement(sql);
				for (int i = 1; i <= values.size(); i++) {
					statement.setObject(i,values.get(i-1));
				}
				statement.setObject(values.size()+1,filtervalue);
				statement.addBatch();
				int[] result = statement.executeBatch();
				logger.info("BATCH UPDATE INTO '{}' - '{}' lines ", struct, result.length);  
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error("SQL Error in preparedStatement");
			}
		}
		else{
			logger.error("Can't perform update, wrong database type, need relational (mariadb or sqlite) database. Database : '{}' , type : '{}' ",credentials.getDbName(), credentials.getDbType());
			throw new RuntimeException("Can't perform update, wrong database type, need document db");
		}
	}

	private static Connection getJDBCConnection(DBCredentials credentials) {
		try {
			if (connection == null) {
				connection = DriverManager.getConnection("jdbc:mysql://" + credentials.getUrl() + ":" + credentials.getPort() + "/" + credentials.getDbName(), credentials.getUserName(), credentials.getUserPwd());
			}
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("Immpossible to connect to relational db [{} - {} : {}] ", credentials.getDbName(), credentials.getUrl(),credentials.getPort());
		}
		return connection;
	}

	
}
