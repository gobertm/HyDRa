package be.unamur;

import java.sql.SQLException;

public class MainInit {

    public static void main(String args[]) throws SQLException {
        // For '3-dbs.pml model"
        RedisDataInit redisDataInit = new RedisDataInit("localhost", 6363);
        redisDataInit.deleteAll("");
        redisDataInit.persistData(1,10);

        SQLDataInit sqlinit = new SQLDataInit("localhost","3307","mydb","root","password");
//        sqlinit.deleteAll("mydb");
//        sqlinit.createDatabase("mydb");
//        sqlinit.initStructure(1,"mydb");
//        sqlinit.persistData(30,1);
//        sqlinit.getConnection().close();

        // Mongo DB 2
        String mongohost2 = "localhost";
        String mongodbname2 = "mymongo2";
        int mongoport2 = 27100;
        MongoDataInit mongoDataInit2 = new MongoDataInit(mongodbname2, mongohost2, mongoport2, 20);
        mongoDataInit2.deleteAll(mongodbname2);
        mongoDataInit2.setSqlDB(sqlinit);
        mongoDataInit2.persistDataPmlModel(2,false,99);

    }
}
