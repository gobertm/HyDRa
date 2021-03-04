package be.unamur;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MainInit {
    static final Logger logger = LoggerFactory.getLogger(MainInit.class);
    private final static String outpath = "src/main/resources/imdb/";
    private RedisDataInit redisDataInit;
    private SQLDataInit sqlDataInit;
    private MongoDataInit mongoDataInit;

    public MainInit(RedisDataInit redisDataInit, SQLDataInit sqlDataInit, MongoDataInit mongoDataInit) {
        this.redisDataInit = redisDataInit;
        this.sqlDataInit = sqlDataInit;
        this.mongoDataInit = mongoDataInit;
    }

    public static void main(String args[]) throws SQLException {
        String redisUrl, sqlUrl, mongoUrl;
        int redisport, sqlport, mongoport;
        redisUrl="localhost";
        sqlUrl = "localhost";
        mongoUrl = "localhost";
        String mongodbname = "mymongo";
        // Init IMDB
        redisport=6379;
        mongoport=27100;
        sqlport=3307;
        // Init other
//        redisport = 6363;
//        mongoport=27000;
//        sqlport=3310;
        RedisDataInit redisDataInit = new RedisDataInit(redisUrl, redisport);
        SQLDataInit sqlinit = new SQLDataInit(sqlUrl,""+sqlport,"mydb","root","password");
        MongoDataInit mongoDataInit2 = new MongoDataInit(mongodbname, mongoUrl, mongoport, 20);
        mongoDataInit2.setSqlDB(sqlinit);
        mongoDataInit2.setRedis(redisDataInit);
        MainInit mainInit = new MainInit(redisDataInit,sqlinit,mongoDataInit2);

        // for Model 'onetomanyMongoToRel.pml'
//        mainInit.initOneToManyMongoToRel();
        // For '3-dbs.pml model"
//        mainInit.init3DBMS();
        // 'kv-embedded.pml'
//        mainInit.initKVEmbedded();
        //'kv-manytoone.pml'
//        mainInit.initKVManytoone();

        // 'imdb.pml' constructed based on files taken from https://www.imdb.com/interfaces/
        mainInit.initIMDB();
    }

    private void initIMDB() {
        mongoDataInit.deleteAll("actorCollection");
//        redisDataInit.deleteAll();
//        sqlDataInit.deleteDataImdb("directorTable","directed");
//        sqlDataInit.initIMDBStructure("directorTable","directed");
//        processTitleBasicsFile("src/main/resources/imdb/title-basics.tsv","directorCollection");
        processNamesFile("src/main/resources/imdb/name-basics.tsv", "actorCollection", "directorTable","directed");
    }


    private void processNamesFile(String path, String actorCollection, String directorTable, String joinTable) {
        //Note : The column 'knowForTitles' at index 5, contains id of title (not limited to movies)
        logger.info("Starting initialise of IMDB data. Actors and Directors");
        try {
            String[] lineItems;
            int i=0;
            List<String> namesFile = Files.readAllLines(Paths.get(path));
            logger.debug("All lines loaded");
            List<String[]> directors = new ArrayList<>();
            List<String[]> actors = new ArrayList<>();
            for (String nameLine : namesFile) {
                lineItems = nameLine.split("\t");
                if (lineItems[4].contains("director"))
                    directors.add(lineItems);
                else if(lineItems[4].contains("actor") || lineItems[4].contains("actress"))
                    actors.add(lineItems);
                i++;
                if(i%50000==0)
                    logger.debug("Processed {} lines",i);
            }
            mongoDataInit.addActors(actors,actorCollection);
//            sqlDataInit.addPersonToTable(directors, directorTable, joinTable);
            logger.info("Inserted data director, actor and work of director.");
        } catch (FileNotFoundException e) {
            logger.error("Can't open file ");
            e.printStackTrace();
        } catch (IOException e) {
            logger.error("Error while reading line");
            e.printStackTrace();
        }

    }

    private void processTitleBasicsFile(String path, String collectionWithMovies) {
        logger.info("Starting initialise of IMDB data. Movies");
        try {
            BufferedReader tsv = new BufferedReader(new FileReader(path));
            String line = null;
            String[] lineItems;
            int i = 0;
            List<String> titleFile = Files.readAllLines(Paths.get(path));
            logger.debug("All lines loaded");
            List<String[]> movies = new ArrayList<>();
            for (String movieLine : titleFile) {
                lineItems = movieLine.split("\t");
                if (lineItems[1].contains("movie")) {
                    movies.add(lineItems);
                    i++;
                }
            }
            logger.debug("Found {} movies ",i);
            redisDataInit.addMovie(movies);
        } catch (FileNotFoundException e) {
            logger.error("Error opening file");
            e.printStackTrace();
        } catch (IOException e) {
            logger.error("Error reading file");
            e.printStackTrace();
        }
    }

    private void initOneToManyMongoToRel() throws SQLException {
//                sqlDataInit.deleteAll("mydb");
//        sqlDataInit.createDatabase("mydb");
//        sqlDataInit.initStructure(PmlModelEnum.ONETOMANYPML,"mydb");
        sqlDataInit.persistData(30,PmlModelEnum.ONETOMANYPML);
        mongoDataInit.dropDatabase();
        mongoDataInit.persistDataPmlModel(2,true,PmlModelEnum.ONETOMANYMONGOTOREL);
    }

    private void initKVEmbedded() {
        redisDataInit.deleteAll();
        redisDataInit.persistData(PmlModelEnum.KVEMBEDDED,20);
    }

    private void initKVManytoone() {
        redisDataInit.deleteAll();
        redisDataInit.persistData(PmlModelEnum.KVMANYTOONE,20);
    }

    private void init3DBMS() throws SQLException {
        redisDataInit.deleteAll();
        redisDataInit.persistData(PmlModelEnum.ALLDBS,10);

//        sqlDataInit.deleteAll("mydb");
//        sqlDataInit.createDatabase("mydb");
        sqlDataInit.initStructure(PmlModelEnum.ALLDBS,"mydb");
        sqlDataInit.persistData(30,PmlModelEnum.ALLDBS);
        sqlDataInit.getConnection().close();

        // Mongo DB 2
        mongoDataInit.dropDatabase();
        mongoDataInit.persistDataPmlModel(2,false,null);

    }
}
