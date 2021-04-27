package dbconnection;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class DBConnectionMgr {

	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DBConnectionMgr.class);
	private static Map<String, DBConnection> mapDB = new HashMap<String, DBConnection>(); 

	static {
		try{
				mapDB.put("mydb", new RelationalDBConnection("localhost", "3307", "mydb", "root", "password"));
		} catch(Exception e) {
			logger.error("Error in DBConnection Mgr creation");
			e.printStackTrace();
		}
	}

	public static Map<String, DBConnection> getMapDB(){
		return mapDB;
	}
	
}
