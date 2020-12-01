package be.unamur;

import java.util.Iterator;
import java.util.Map;

public interface CypherExecutor {
    Iterator query(String statement, Map<String,Object> params);
    void close();
}