package be.unamur;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;

public class Neo4jDataInit {

    static final Logger logger = LoggerFactory.getLogger(Neo4jDataInit.class);
    private CypherExecutor cypherExecutor;
    String query = "MATCH (:Movie {title:{1}})<-[:ACTED_IN]-(a:Person) RETURN a.name as actor";

    public Neo4jDataInit(String neohost, int neoport, String neouser, String neopwd, CypherExecutorType executorType) throws SQLException {
        if(executorType == CypherExecutorType.JDBC_EXEC)
            cypherExecutor = new JdbcCypherExecutor(neohost, neoport, neouser, neopwd);
        if(executorType == CypherExecutorType.JAVADRIVER_EXEC)
            cypherExecutor = new Neo4JJavaDriverCypherExecutor(neohost, neoport, neouser, neopwd);
    }

    public void createProducts(String url){
        logger.info("Initialising products in Graph database");
        String query = "LOAD CSV WITH HEADERS FROM $1 AS row\n" +
                "CREATE (n:Product)\n" +
                "SET n = row,\n" +
                "n.unitPrice = toFloat(row.unitPrice),\n" +
                "n.unitsInStock = toInteger(row.unitsInStock), n.unitsOnOrder = toInteger(row.unitsOnOrder),\n" +
                "n.reorderLevel = toInteger(row.reorderLevel), n.discontinued = (row.discontinued <> \"0\")\n";
        cypherExecutor.query(query, Map.of("1",url));
    }
    public void createCategories(String url){
        logger.info("Initialising categories in Graph database");
        String query = "LOAD CSV WITH HEADERS FROM $1 AS row\n" +
                "CREATE (n:Category)\n" +
                "SET n = row\n";
        cypherExecutor.query(query, Map.of("1",url));
    }

    public void createSuppliers(String url){
        logger.info("Initialising suppliers in Graph database");
        String query = "LOAD CSV WITH HEADERS FROM $1 AS row\n" +
                "CREATE (n:Supplier)\n" +
                "SET n = row\n";
        cypherExecutor.query(query, Map.of("1",url));
    }

    public void getAllProducts() {
        logger.info("Getting all products in neo4j database");
        String query = "MATCH (n:Products) return n";
        cypherExecutor.query(query,Map.of());
    }

    public void createRelationships() {
        logger.info("Create Relationships in graph");
        String query = "MATCH (p:Product),(c:Category)\n" +
                "WHERE p.categoryID = c.categoryID\n" +
                "CREATE (p)-[:PART_OF]->(c)\n";
        cypherExecutor.query(query,Map.of());
        query = "MATCH (p:Product),(s:Supplier)\n" +
                "WHERE p.supplierID = s.supplierID\n" +
                "CREATE (s)-[:SUPPLIES]->(p)\n";
        cypherExecutor.query(query, Map.of());
    }

    public void close() {
        cypherExecutor.close();
    }
}
