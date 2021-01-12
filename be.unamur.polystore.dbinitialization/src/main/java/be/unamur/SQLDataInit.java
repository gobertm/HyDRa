package be.unamur;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class SQLDataInit {
    static final Logger logger = LoggerFactory.getLogger(SQLDataInit.class);
    private String databasename;
    private String login;
    private String password;
    private String localhost;
    private String port;
    private Connection connection;
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
        sqlinit.createConnection();
        // pml 'simplehybrid'
//        sqlinit.initStructure(HYBRIDPML);
//        sqlinit.initData(200);

        // pml 'simpleonetomanyrel'
        sqlinit.initStructure(ONETOMANYPML);
        sqlinit.initData(30,ONETOMANYPML);
        sqlinit.getConnection().close();


    }

    public void initStructure(int pmlmodel) throws SQLException {
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

    public void initData(int numberofrecords, int pmlmodel) throws SQLException {
        Statement stmt=connection.createStatement();
        int insertedProduct = 0,insertedReviews=0;
        for (int i = 0; i < numberofrecords; i++) {
            try {
                if (pmlmodel == HYBRIDPML) {
                    stmt.execute("insert into ProductCatalogTable(product_id,europrice, description, categoryname) VALUES ('product" + i + "'," + RandomUtils.nextInt() + ",'desc','" + RandomStringUtils.random(2, 65, 70, true, false) + "')");
                    insertedProduct++;
                }
                if (pmlmodel == ONETOMANYPML) {
                    stmt.execute("insert into ProductCatalogTable(product_id,europrice, description) VALUES ('product" + i + "'," + RandomUtils.nextInt() + ",'desc')");
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
    }

    public void createConnection() {
        try {
            connection = DriverManager.getConnection("jdbc:mysql://" + localhost + ":" + port + "/" + databasename, login, password);
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

    public void executeSQL(String query) {
        Statement stmt= null;
        try {
            stmt = connection.createStatement();
            stmt.execute(query);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
