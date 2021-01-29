package be.unamur;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class SQLDataInit implements DataInit{
    static final Logger logger = LoggerFactory.getLogger(SQLDataInit.class);
    private String databasename;
    private String login;
    private String password;
    private String localhost;
    private String port;
    private Connection connection;
    private Connection connection2;
    private static final int HYBRIDPML=0, ONETOMANYPML=1;

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
        sqlinit.initStructure(ONETOMANYPML,"mydb");
        sqlinit.persistData(30,ONETOMANYPML);
        sqlinit.getConnection().close();


    }



    public void initStructure(int pmlmodel, String dbname) throws SQLException {
        if(connection==null)
            initConnection();
        Statement stmt=connection.createStatement();
        if (pmlmodel == HYBRIDPML) {
            stmt.execute("create table IF NOT EXISTS ProductCatalogTable (" +
                    "product_id char(36)," +
                    "europrice char(36)," +
                    "description char(50)," +
                    "categoryname char(5))");
            logger.info("Structure tables in database created");
        }
        if (pmlmodel == ONETOMANYPML) {
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
    }

    public void createDatabase(String dbname) {
        initConnectionWithoutDB();
        Statement stmt= null;
        try {
            stmt = connection2.createStatement();
            stmt.execute("CREATE DATABASE IF NOT EXISTS "+dbname);
            logger.info("Created database {}", databasename);
            connection2.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public void persistData(int numberofrecords, int pmlmodel) {
        if(connection==null)
            initConnection();
        Statement stmt= null;
        try {
            stmt = connection.createStatement();
        int insertedProduct = 0,insertedReviews=0;
        for (int i = 0; i < numberofrecords; i++) {
            try {
                if (pmlmodel == HYBRIDPML) {
                    stmt.execute("insert into ProductCatalogTable(product_id,europrice, description, categoryname) VALUES ('product" + i + "'," + RandomUtils.nextInt()+"€" + ",'desc','" + RandomStringUtils.random(2, 65, 70, true, false) + "')");
                    insertedProduct++;
                }
                if (pmlmodel == ONETOMANYPML) {
                    stmt.execute("insert into ProductCatalogTable(product_id,europrice, description) VALUES ('product" + i + "','" + RandomUtils.nextInt()+"€'" + ",'desc')");
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
}
