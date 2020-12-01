package be.unamur;

import org.neo4j.driver.*;
import java.util.*;

public class Neo4JJavaDriverCypherExecutor implements CypherExecutor{
    private Session session;
    private Driver driver;


    public Neo4JJavaDriverCypherExecutor(String host, int port, String username, String password) {
        driver = GraphDatabase.driver(
                "bolt://"+host+":"+port, AuthTokens.basic(username, password));
        session = driver.session();
    }

    public Result query(String query, Map<String, Object> params){
        return session.run(query, params);
    }

    public void close() {
        session.close();
        driver.close();
    }
}

