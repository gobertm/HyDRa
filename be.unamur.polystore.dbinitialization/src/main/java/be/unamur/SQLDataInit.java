package be.unamur;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Arrays;

public class SQLDataInit {
    static final Logger logger = LoggerFactory.getLogger(SQLDataInit.class);
    private String databasename;
    private String login;
    private String password;
    private String localhost;
    private String port;
    private Connection connection;
    private Connection connection2;
//    private static final int HYBRIDPML=0, ONETOMANYPML=1;

    public SQLDataInit(String localhost, String port, String databasename, String login, String password) {
        this.localhost=localhost;
        this.port=port;
        this.databasename=databasename;
        this.login=login;
        this.password=password;
    }



    public static void main(String args[]) throws SQLException {
        SQLDataInit sqlinit = new SQLDataInit("localhost","3307","mydb","root","password");
        sqlinit.initConnection();
        // pml 'simplehybrid'
//        sqlinit.initStructure(HYBRIDPML);
//        sqlinit.initData(200);

        // pml 'simpleonetomanyrel'
        sqlinit.initStructure(PmlModelEnum.ONETOMANYPML,"mydb");
        sqlinit.persistData(30,PmlModelEnum.ONETOMANYPML);
        sqlinit.getConnection().close();


    }



    public void initStructure(PmlModelEnum pmlmodel, String dbname) throws SQLException {
        if(connection==null)
            initConnection();
        Statement stmt=connection.createStatement();
        if (pmlmodel == PmlModelEnum.SIMPLEHYBRID) {
            stmt.execute("create table IF NOT EXISTS ProductCatalogTable (" +
                    "product_id char(36)," +
                    "europrice char(36)," +
                    "description char(50)," +
                    "categoryname char(5))");
            logger.info("Structure tables in database created");
        }
        if (pmlmodel == PmlModelEnum.ONETOMANYPML ) {
            stmt.execute("create table IF NOT EXISTS ProductCatalogTable (" +
                    "product_id char(36) primary key," +
                    "europrice char(36)," +
                    "description char(50)" +
                    ")");
            stmt.execute("create table IF NOT EXISTS ReviewTable(" +
                    "review_id char(36)," +
                    "rating int," +
                    "content char(240)," +
                    "product_ref  char(36)" +
                    ")");
            logger.info("Structure tables in database created");
        }
        if (pmlmodel == PmlModelEnum.ALLDBS) {
            stmt.execute("create table IF NOT EXISTS ProductCatalogTable (" +
                    "product_id char(36) primary key," +
                    "europrice char(36)," +
                    "description char(50)" +
                    ")");
            logger.info("Structure tables in database created");
        }
    }

    public void createDatabase(String dbname) {
        initConnectionWithoutDB();
        Statement stmt= null;
        try {
            stmt = connection2.createStatement();
            stmt.execute("CREATE DATABASE IF NOT EXISTS "+dbname+" CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci");
            logger.info("Created database {}", databasename);
            connection2.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public void persistData(int numberofrecords, PmlModelEnum pmlmodel) {
        if(connection==null)
            initConnection();
        Statement stmt= null;
        try {
            stmt = connection.createStatement();
        int insertedProduct = 0,insertedReviews=0;
        for (int i = 0; i < numberofrecords; i++) {
            try {
                if (pmlmodel == PmlModelEnum.SIMPLEHYBRID) {
                    stmt.execute("insert into ProductCatalogTable(product_id,europrice, description, categoryname) VALUES ('product" + i + "'," + RandomUtils.nextInt()+"€" + ",'desc','" + RandomStringUtils.random(2, 65, 70, true, false) + "')");
                    insertedProduct++;
                }
                if (pmlmodel == PmlModelEnum.ALLDBS) {
                    stmt.execute("insert into ProductCatalogTable(product_id,europrice, description) VALUES ('product" + i + "','" + RandomUtils.nextInt()+"€'" + ",'"+RandomStringUtils.randomAlphabetic(12)+"')");
                    insertedProduct++;
                }
                if (pmlmodel == PmlModelEnum.ONETOMANYPML) {
                    stmt.execute("insert into ProductCatalogTable(product_id,europrice, description) VALUES ('product" + i + "','" + RandomUtils.nextInt()+"€'" + ",'"+RandomStringUtils.randomAlphabetic(12)+"')");
                    insertedProduct++;
                    stmt.execute("insert into ReviewTable(review_id, rating, content, product_ref) VALUES ('review"+i+"',"+RandomUtils.nextInt(0,5)+",'"+RandomStringUtils.randomAlphabetic(60)+"','product" + RandomUtils.nextInt(0,numberofrecords) + "')");
                    insertedReviews++;
                }
            } catch (SQLIntegrityConstraintViolationException e) {
                logger.warn("Duplicate entry. Skipping row..");
            }
        }
        logger.info("Data [{}] rows inserted in table ProductCatalogTable", insertedProduct);
        logger.info("Data [{}] rows inserted in table ReviewTable", insertedReviews);
        } catch (SQLException e) {
            logger.error("Unexpected SQLException");
            e.printStackTrace();
        }
    }

    public void initConnection() {
        try {
            connection = DriverManager.getConnection("jdbc:mysql://" + localhost + ":" + port + "/" + databasename, login, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    private void initConnectionWithoutDB() {
        try {
            connection2 = DriverManager.getConnection("jdbc:mysql://" + localhost + ":" + port , login, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public void deleteAll(String dbname){
        if(connection2==null)
            initConnectionWithoutDB();
        logger.info("Dropping database [{}]", dbname);
        executeSQL("DROP DATABASE "+dbname);
    }

    public void executeSQL(String query) {
        if(connection==null)
            initConnection();
        Statement stmt= null;
        try {
            stmt = connection.createStatement();
            stmt.execute(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void addActorToTable(String[] actor) {
        if(connection==null)
            initConnection();
        try {
            logger.debug("Inserting actor {}", actor[0]);
            PreparedStatement statement = connection.prepareStatement("INSERT INTO actorTable (id, fullname, birth, death) VALUES (?,?,?,?)");
            statement.setObject(1,actor[0]);
            statement.setObject(2,actor[1]);
            if (actor[2].contains("\\N")) {
                statement.setObject(3,null);
            }else
                statement.setObject(3,actor[2]);
            if (actor[3].contains("\\N")) {
                statement.setObject(4,null);
            }else
                statement.setObject(4,actor[3]);
            statement.execute();

            statement = connection.prepareStatement("INSERT INTO role (actor_id, movie_id) VALUES (?,?)");
            String[] titles = actor[5].split(",");
            for (String titleId : Arrays.asList(titles)) {
                statement.clearParameters();
                statement.setObject(1, actor[0]);
                statement.setObject(2,titleId);
                statement.addBatch();
            }
            statement.executeBatch();
            logger.debug("Inserted roles {} - {}",actor[0],titles);
        } catch (SQLException e) {
            logger.error("SQLException error");
            e.printStackTrace();
        }
    }

    public void initIMDBStructure() {
        logger.info("Creating IMDB data tables");
        if(connection==null)
            initConnection();
        try {
            Statement stmt=connection.createStatement();
            stmt.execute("CREATE TABLE IF NOT EXISTS actorTable(" +
                    "id varchar(20)," +
                    "fullname char(50)," +
                    "birth int," +
                    "death int" +
                    ")");
            stmt.execute("CREATE TABLE IF NOT EXISTS role(" +
                    "actor_id char(20)," +
                    "movie_id char(20)" +
                    ")");
        } catch (SQLException e) {
            logger.error("SQLException error");
            e.printStackTrace();
        }
    }

    public void deleteDataImdb() {
        logger.info("Deleting data in IMDB tables ");
        if(connection==null)
            initConnection();
        try{
            Statement stmt = connection.createStatement();
            stmt.execute("DELETE FROM role");
            stmt.execute("DELETE FROM actorTable");
        } catch (SQLException e) {
            logger.error("SQLException error");
            e.printStackTrace();
        }
    }
}
