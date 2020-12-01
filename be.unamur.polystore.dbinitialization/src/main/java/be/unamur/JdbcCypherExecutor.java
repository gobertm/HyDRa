package be.unamur;


import java.sql.*;
import java.util.*;

public class JdbcCypherExecutor implements CypherExecutor {

    private final Connection conn;

    public JdbcCypherExecutor(String host, int port, String username, String password) {
        try {
            conn = DriverManager.getConnection("jdbc:neo4j:http://"+host+":"+port,username,password);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<Map<String, Object>> query(String query, Map<String, Object> params) {
        try {
            final PreparedStatement statement = conn.prepareStatement(query);
            setParameters(statement, params);
            final ResultSet result = statement.executeQuery();
            return new Iterator<Map<String, Object>>() {

                boolean hasNext = result.next();
                public List<String> columns;

                @Override
                public boolean hasNext() {
                    return hasNext;
                }

                private List<String> getColumns() throws SQLException {
                    if (columns != null) return columns;
                    ResultSetMetaData metaData = result.getMetaData();
                    int count = metaData.getColumnCount();
                    List<String> cols = new ArrayList<>(count);
                    for (int i = 1; i <= count; i++) cols.add(metaData.getColumnName(i));
                    return columns = cols;
                }

                @Override
                public Map<String, Object> next() {
                    try {
                        if (hasNext) {
                            Map<String, Object> map = new LinkedHashMap<>();
                            for (String col : getColumns()) map.put(col, result.getObject(col));
                            hasNext = result.next();
                            if (!hasNext) {
                                result.close();
                                statement.close();
                            }
                            return map;
                        } else throw new NoSuchElementException();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void remove() {
                }
            };
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {

    }

    private void setParameters(PreparedStatement statement, Map<String, Object> params) throws SQLException {
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            int index = Integer.parseInt(entry.getKey());
            statement.setObject(index, entry.getValue());
        }
    }
}