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

    public SQLDataInit(String localhost, String port, String databasename, String login, String password) {
        this.localhost=localhost;
        this.port=port;
        this.databasename=databasename;
        this.login=login;
        this.password=password;
    }

    public void initData(int numberofrecords) throws SQLException {
        Statement stmt=connection.createStatement();
        for (int i = 100; i < numberofrecords; i++) {
            stmt.execute("insert into ProductCatalogTable(product_id,europrice, description, categoryname) VALUES ('product" + i + "','" + RandomUtils.nextInt() + "€','desc','"+ RandomStringUtils.random(2,65,70,true,false)+"')");
        }
    }


    public static void main(String args[]) throws SQLException {
        SQLDataInit sqlinit = new SQLDataInit("localhost","3307","mydb","root","password");
        sqlinit.createConnection();
        sqlinit.initStructure();
        sqlinit.initData(200);
        sqlinit.getConnection().close();
    }

    private void initStructure() throws SQLException {
        Statement stmt=connection.createStatement();
        stmt.execute("create table IF NOT EXISTS ProductCatalogTable (" +
                "product_id char(36)," +
                "europrice char(36)," +
                "description char(50)," +
                "categoryname char(5))");
        logger.info("Structure tables in database created");
    }

    private void createConnection() {
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
}
