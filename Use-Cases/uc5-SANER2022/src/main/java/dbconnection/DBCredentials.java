package dbconnection;

import java.util.HashMap;
import java.util.Map;


public class DBCredentials {
    private String dbName;
    private String url;
    private int port;
    private String userName;
    private String userPwd;
    private String dbType;
	private static Map<String, DBCredentials> dbPorts = new HashMap<String, DBCredentials>();

    protected DBCredentials(String dbName, String url, int port, String userName, String userPwd, String dbType) {
        this.dbName = dbName;
        this.url = url;
        this.port = port;
        this.userName = userName;
        this.userPwd = userPwd;
        this.dbType = dbType;
    }
	
	static {
			dbPorts.put("mysqlbench", new DBCredentials("mysqlbench", "localhost", 3310, "root", "password","mariadb"));
			dbPorts.put("redisbench", new DBCredentials("", "localhost", 6363, "", "","redis"));
			dbPorts.put("mongobench", new DBCredentials("", "localhost", 27000, "", "","mongodb"));
	}

	public static Map<String, DBCredentials> getDbPorts() {
        return dbPorts;
    }


    public String getDbName() {
        return dbName;
    }

    public String getUrl() {
        return url;
    }

    public int getPort() {
        return port;
    }

    public String getUserName() {
        return userName;
    }

    public String getUserPwd() {
        return userPwd;
    }

    public String getDbType() {
        return dbType;
    }
}

