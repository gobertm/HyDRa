package be.unamur;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
        // Init
        RedisDataInit redisDataInit = new RedisDataInit("localhost", 6379);
        SQLDataInit sqlinit = new SQLDataInit("localhost","3307","mydb","root","password");
        String mongohost2 = "localhost";
        String mongodbname2 = "mymongo";
        int mongoport2 = 27100;
        MongoDataInit mongoDataInit2 = new MongoDataInit(mongodbname2, mongohost2, mongoport2, 20);
        mongoDataInit2.setSqlDB(sqlinit);
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
        mongoDataInit.deleteAll();
        sqlDataInit.deleteDataImdb();
        sqlDataInit.initIMDBStructure();
        processNamesFile("src/main/resources/imdb/name-basics.tsv");
        processTitleBasicsFile("src/main/resources/imdb/title-basics.tsv","directorCollection");
    }


    private void processNamesFile(String path) {
        //Note : The column 'knowForTitles' at index 5, contains id of title (not limited to movies)
        logger.info("Starting initialise of IMDB data. Actors and Directors");
        try {
            BufferedReader tsv = new BufferedReader(new FileReader(path));
            String line =null;
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
                if(i%10000==0)
                    logger.debug("Processed {} lines",i);
            }
            mongoDataInit.addDirector(directors);
            sqlDataInit.addActorToTable(actors);
            logger.info("Inserted data indirector collection, actorTable and role table.");
        } catch (FileNotFoundException e) {
            logger.error("Can't open file ");
            e.printStackTrace();
        } catch (IOException e) {
            logger.error("Error while reading line");
            e.printStackTrace();
        }

    }

    private void processTitleBasicsFile(String path,String directorCollection) {
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
            mongoDataInit.updateDirectorMovieInfo(movies,directorCollection);
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
        mongoDataInit.deleteAll();
        mongoDataInit.persistDataPmlModel(2,true,PmlModelEnum.ONETOMANYMONGOTOREL);
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
