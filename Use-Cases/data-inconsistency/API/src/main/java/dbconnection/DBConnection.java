package dbconnection;

public interface DBConnection {
	int insertOrUpdateOrDelete(String query, java.util.List<Object> inputs);
}
