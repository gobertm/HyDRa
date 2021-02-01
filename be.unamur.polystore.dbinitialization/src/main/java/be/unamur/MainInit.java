package be.unamur;

import com.mongodb.client.MongoCollection;

import java.sql.SQLException;

public class MainInit {

    private RedisDataInit redisDataInit;
    private SQLDataInit sqlDataInit;
    private MongoDataInit mongoDataInit;

    public MainInit(RedisDataInit redisDataInit, SQLDataInit sqlDataInit, MongoDataInit mongoDataInit) {
        this.redisDataInit = redisDataInit;
        this.sqlDataInit = sqlDataInit;
        this.mongoDataInit = mongoDataInit;
    }

    public static void main(String args[]) throws SQLException {
        // Init
        RedisDataInit redisDataInit = new RedisDataInit("localhost", 6363);
        SQLDataInit sqlinit = new SQLDataInit("localhost","3307","mydb","root","password");
        String mongohost2 = "localhost";
        String mongodbname2 = "mymongo2";
        int mongoport2 = 27100;
        MongoDataInit mongoDataInit2 = new MongoDataInit(mongodbname2, mongohost2, mongoport2, 20);
        mongoDataInit2.setSqlDB(sqlinit);
        MainInit mainInit = new MainInit(redisDataInit,sqlinit,mongoDataInit2);

        // For '3-dbs.pml model"
//        mainInit.init3DBMS();
        // 'kv-embedded.pml'
//        mainInit.initKVEmbedded();
        //'kv-manytoone.pml'
        mainInit.initKVManytoone();
    }

    private void initKVEmbedded() {
        redisDataInit.deleteAll("");
        redisDataInit.persistData(PmlModelEnum.KVEMBEDDED,20);
    }

    private void initKVManytoone() {
        redisDataInit.deleteAll("");
        redisDataInit.persistData(PmlModelEnum.KVMANYTOONE,20);
    }

    private void init3DBMS() throws SQLException {
        redisDataInit.deleteAll("");
        redisDataInit.persistData(PmlModelEnum.ALLDBS,10);

//        sqlDataInit.deleteAll("mydb");
//        sqlDataInit.createDatabase("mydb");
        sqlDataInit.initStructure(PmlModelEnum.ALLDBS,"mydb");
        sqlDataInit.persistData(30,PmlModelEnum.ALLDBS);
        sqlDataInit.getConnection().close();

        // Mongo DB 2
        mongoDataInit.deleteAll();
        mongoDataInit.persistDataPmlModel(2,false,null);

    }
}
