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
	private static Map<String, DBCredentials> dbPorts = new HashMap<String, DBCredentials>();
	private static MongoClient mongoClient;
	private static Connection connection;

	private static class DBCredentials {

		protected String dbName;
		protected String url;
		protected int port;
		protected String userName;
		protected String userPwd;
		protected String dbType;

		protected DBCredentials(String dbName, String url, int port, String userName, String userPwd, String dbType) {
			this.dbName = dbName;
			this.url = url;
			this.port = port;
			this.userName = userName;
			this.userPwd = userPwd;
			this.dbType = dbType;
		}

	}

	static {
			dbPorts.put("mydb", new DBCredentials("mydb", "localhost", 3307, "root", "password","mariadb"));
			dbPorts.put("myredis", new DBCredentials("", "localhost", 6379, "", "","redis"));
			dbPorts.put("mymongo", new DBCredentials("", "localhost", 27100, "", "","mongodb"));
	}

	public static Map<String, DBConnection> getMapDB(){
		return mapDB;
	}

	public static void upsertMany(Bson filter, Bson updateOp, String struct, String db){
		DBCredentials credentials = dbPorts.get(db);
		if(credentials.dbType.equals("mongodb")){ 
			MongoDatabase mongoDatabase = getMongoClient(credentials).getDatabase(db);
	        MongoCollection<Document> structCollection = mongoDatabase.getCollection(struct);
			UpdateOptions options = new UpdateOptions().upsert(true);
	        structCollection.updateMany(filter, updateOp, options);
		}
		else{
			logger.error("Can't perform update, wrong database type, need document db. Database : '{}' , type : '{}' ",credentials.dbName, credentials.dbType);
			throw new RuntimeException("Can't perform update, wrong database type, need document db");
		}
	}

	public static void upsertMany(Bson filter, Bson updateOp, List<Bson> arrayFiltersConditions, String struct, String db){
		DBCredentials credentials = dbPorts.get(db);
		if(credentials.dbType.equals("mongodb")){ 
			MongoDatabase mongoDatabase = getMongoClient(credentials).getDatabase(db);
	        MongoCollection<Document> structCollection = mongoDatabase.getCollection(struct);
			UpdateOptions updateOptions = new UpdateOptions().arrayFilters(arrayFiltersConditions);
	        structCollection.updateMany(filter, updateOp, updateOptions);
		}
		else{
			logger.error("Can't perform update, wrong database type, need document db. Database : '{}' , type : '{}' ",credentials.dbName, credentials.dbType);
			throw new RuntimeException("Can't perform update, wrong database type, need document db");
		}
	}


	public static void insertMany(List<Document> documents, String struct, String db) {
		DBCredentials credentials = dbPorts.get(db);
		if(!credentials.dbType.equals("mongodb")){
			logger.error("Can't perform update, wrong database type, need document db. Database : '{}' , type : '{}' ",credentials.dbName, credentials.dbType);
			throw new RuntimeException("Can't perform update, wrong database type, need document db");
		}
		else{
			MongoDatabase mongoDatabase = getMongoClient(credentials).getDatabase(db);
			MongoCollection<Document> structCollection = mongoDatabase.getCollection(struct);
			structCollection.insertMany(documents);
		}
	}

	private static MongoClient getMongoClient(DBCredentials credentials) {
		if (mongoClient == null) {
			mongoClient = MongoClients.create(
					MongoClientSettings.builder()
							.applyToClusterSettings(builder ->
									builder.hosts(Arrays.asList(new ServerAddress(credentials.url, credentials.port))))
							.build());
		}
		return mongoClient;
	}

	public static void insertInTable(List<String> columns, List<List<Object>> rows, String struct, String db) {
		DBCredentials credentials = dbPorts.get(db);
		if((credentials.dbType.equals("mariadb") || credentials.dbType.equals("sqlite"))){
			try {
				String sql = "INSERT INTO " + struct + "("+String.join(",",columns)+") VALUES ("+String.join(",", Collections.nCopies(columns.size(),"?"))+")";
				PreparedStatement statement = getJDBCConnection(credentials).prepareStatement(sql);
				for (List<Object> row : rows) {
					for (int i = 1; i <= row.size(); i++) {
						statement.setObject(i,row.get(i-1));
					}
					statement.addBatch();
				}
				statement.executeBatch();
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error("SQL Error in preparedStatement");
			}
		}
		else{
			logger.error("Can't perform update, wrong database type, need relational (mariadb or sqlite) database. Database : '{}' , type : '{}' ",credentials.dbName, credentials.dbType);
			throw new RuntimeException("Can't perform update, wrong database type, need document db");
		}
	}

	private static Connection getJDBCConnection(DBCredentials credentials) {
		try {
			if (connection == null) {
				connection = DriverManager.getConnection("jdbc:mysql://" + credentials.url + ":" + credentials.port + "/" + credentials.dbName, credentials.userName, credentials.userPwd);
			}
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("Immpossible to connect to relational db [{} - {} : {}] ", credentials.dbName, credentials.url,credentials.port);
		}
		return connection;
	}

	
}
