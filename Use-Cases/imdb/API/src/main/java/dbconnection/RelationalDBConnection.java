package dbconnection;

import java.sql.SQLException;

public class RelationalDBConnection implements DBConnection{
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RelationalDBConnection.class);
	protected String host;
	protected String port;
	protected String driverClass;
	protected String schemaName;
	protected String userName;
	protected String userPassword;

	protected String jdbcUrl;

	private java.sql.Connection conn = null;

	public RelationalDBConnection(String host, String port, String schemaName, String userName, String userPassword)
			throws ClassNotFoundException, java.sql.SQLException {
		this.host = host;
		this.port = port;
		this.schemaName = schemaName;
		this.userName = userName;
		this.userPassword = userPassword;

		this.jdbcUrl = "jdbc:mysql://" + this.host + ":" + this.port + "/" + this.schemaName;
		this.driverClass = "com.mysql.jdbc.Driver";

		//Class.forName(this.driverClass);
		openConnection();
	}

	public void openConnection(){
		if(this.conn == null)
			try{
			logger.debug("Opening relational DB connection {}", jdbcUrl);
			this.conn = java.sql.DriverManager.getConnection(this.jdbcUrl, this.userName, this.userPassword);
			}catch(SQLException e){
				logger.error("Impossible to connect to DB {}", jdbcUrl);
				System.err.println(e);
				System.exit(1);
			}
	}

	public void closeConnection() {
		try {
			if(conn != null)
				conn.close();
		}catch(SQLException e){
				logger.error("Impossible to close DB connection{}", jdbcUrl);
				System.err.println(e);
			} finally {
			conn = null;
		}
	}

	public java.sql.ResultSet select(String query) throws java.sql.SQLException {
		return select(query, null);
	}

	public java.sql.ResultSet select(String query, java.util.List<Object> inputs) throws java.sql.SQLException {
		openConnection();
		java.sql.PreparedStatement st = null;
		try {
			st = conn.prepareStatement(query);
			int i = 1;
			if (inputs != null)
				for (Object input : inputs) {
					st.setObject(i, input);
					i++;
				}

			return st.executeQuery();
		} catch (java.sql.SQLException e) {
			if (st != null)
				st.close();
			throw e;
		}
	}

	public void closeStatement(java.sql.ResultSet r) throws java.sql.SQLException {
		if (r != null) {
			java.sql.Statement st = r.getStatement();
			if (st != null)
				st.close();
		}
	}

	@Override
	public int insertOrUpdateOrDelete(String query, java.util.List<Object> inputs) {
		java.sql.PreparedStatement st = null;
		try {
			openConnection();
			logger.debug("Executing insert/update/delete statement [{}]",query);
			st = conn.prepareStatement(query);
			int i = 1;
			if (inputs != null)
				for (Object input : inputs) {
					st.setObject(i, input);
					i++;
				}

			return st.executeUpdate();
		}catch(SQLException e){
			logger.error("Error executing insert/update/delete statement");
			System.err.println(e);
			System.exit(1);
		} finally {
			if (st != null)
			try{
				st.close();
			}catch(SQLException e){
				logger.error("Impossible to close statement");
				System.err.println(e);
			}
		}
		return 0;
	}

	public int insertOrUpdateOrDelete(String query) throws java.sql.SQLException {
		return insertOrUpdateOrDelete(query, null);
	}

}

