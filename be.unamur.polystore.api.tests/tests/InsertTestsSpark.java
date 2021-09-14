package generated;

import com.redislabs.provider.redis.ReadWriteConfig;
import com.redislabs.provider.redis.RedisConfig;
import com.redislabs.provider.redis.RedisContext;
import dao.impl.ActorServiceImpl;
import dao.impl.DirectorServiceImpl;
import dao.impl.MovieServiceImpl;
import dao.services.ActorService;
import dao.services.DirectorService;
import dao.services.MovieService;
import dbconnection.SparkConnectionMgr;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.BeforeClass;
import org.junit.Test;
import pojo.Actor;
import pojo.Director;
import pojo.Movie;
import scala.Tuple2;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class InsertTestsSpark {
    static SparkSession spark;


    @BeforeClass
    public static void init() {
        spark = SparkSession.builder().
                appName("documentation").master("local").getOrCreate();
//        spark.sparkContext().setLogLevel("ERROR");
    }

    @Test
    public void testInsertSparkDirectorUC1() {
        List<StructField> listOfStructuField = new ArrayList<StructField>();
        listOfStructuField.add(DataTypes.createStructField("id", DataTypes.StringType, false));
        listOfStructuField.add(DataTypes.createStructField("fullname", DataTypes.StringType, false));
        StructType structType = DataTypes.createStructType(listOfStructuField);

        List<Row> list=new ArrayList<Row>();
        list.add(RowFactory.create("FAKEID", "FAKENAME"));

        Dataset<Row> data = spark.createDataFrame(list, structType);
        data.show();
        data.write()
                .format("jdbc")
                .option("url","jdbc:mysql://localhost:3307/mydb")
                .option("dbtable","directorTable")
                .option("user","root")
                .option("password","password")
                .mode(SaveMode.Append)
                .save();
    }

    @Test
    public void testInsertSparkMongo(){
        List<StructField> listStructFieldActor = new ArrayList<StructField>();
        listStructFieldActor.add(DataTypes.createStructField("id", DataTypes.StringType, false));
        listStructFieldActor.add(DataTypes.createStructField("fullname", DataTypes.StringType, false));
        List<StructField> listStructFieldMovie = new ArrayList<StructField>();
        listStructFieldMovie.add((DataTypes.createStructField("title", DataTypes.StringType, true)));
        listStructFieldMovie.add((DataTypes.createStructField("movieid", DataTypes.StringType, true)));
        StructType movieStructType = DataTypes.createStructType(listStructFieldMovie);
        //array
        ArrayType array = DataTypes.createArrayType(movieStructType);
        listStructFieldActor.add(DataTypes.createStructField("movies", array, true));
        //embedded
//        listStructFieldActor.add(DataTypes.createStructField("movie", movieStructType, true));
        StructType actorStructType = DataTypes.createStructType(listStructFieldActor);

        List<Row> list=new ArrayList<Row>();
        list.add(RowFactory.create("FAKEID", "FAKENAME", Arrays.asList(RowFactory.create("TITLE1", "MOVIEID1"), RowFactory.create("TITLE2", "MOVIEID2"))));

        Dataset<Row> data = spark.createDataFrame(list, actorStructType);
        data.show();
        String mongoURL = "mongodb://localhost:27100/mymongo.actorCollection";
        data.write()
                .format("mongo")
                .option("url","jdbc:mysql://localhost:3307/mydb")
                .option("spark.mongodb.output.uri", mongoURL)
                .option("collection","actorCollection")
                .mode(SaveMode.Append)
                .save();
    }

    @Test
    public void insertSparkRedis() {
        SparkSession spark = SparkSession
                .builder()
                .appName("MyApp")
                .master("local[*]")
                .config("spark.redis.host", "localhost")
                .config("spark.redis.port", "6379")
                .getOrCreate();
        SparkConf sparkConf = new SparkConf()
                .setAppName("MyApp")
                .setMaster("local[*]")
                .set("spark.redis.host", "localhost")
                .set("spark.redis.port", "6379");
        RedisConfig redisConfig = RedisConfig.fromSparkConf(sparkConf);
        ReadWriteConfig readWriteConfig = ReadWriteConfig.fromSparkConf(sparkConf);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        RedisContext redisContext = new RedisContext(jsc.sc());

        List<Tuple2<String, String>> data = Arrays.asList(new Tuple2<String, String>("mykey", "myvalue"),new Tuple2<String,String>("field2","value2"));
        RDD<Tuple2<String, String>> items = jsc.parallelize(data,1).rdd();
        redisContext.toRedisKV(items, 0, redisConfig, readWriteConfig);

        redisContext.toRedisHASH(items, "myhashish",0, redisConfig, readWriteConfig);
    }

    @Test
    public void testInsertDirectorWithSparkConnectionMgr(){
        DirectorService directorService = new DirectorServiceImpl();
        List<Director> directorList = new ArrayList<>();
        for (int i = 0; i<10; i++) {
            Director d = new Director();
            d.setId(""+i);
            d.setLastName("lastname"+i);
            d.setFirstName("firstname"+i);
            d.setYearOfBirth(i);
            directorList.add(d);
            directorService.insertDirector(d);
        }

    }

    @Test
    public void testInsertActorWithSparkConnectionMgr(){
        ActorService actorService = new ActorServiceImpl();
        List<Actor> actorList = new ArrayList<>();
        for (int i = 10; i<20; i++) {
            Actor a = new Actor();
            a.setId(""+i);
            a.setFullName("fullname"+i);
            a.setYearOfBirth("birth"+i);
            a.setYearOfDeath("death"+i);
            actorList.add(a);
            System.out.println(actorService.insertActor(a));
        }
    }

//    @Test
//    public void testInsertMovieWithSparkConnectionMgr() {
//        MovieService movieService = new MovieServiceImpl();
//        List<Movie> movieList = new ArrayList<>();
//        for (int i = 0; i < 10 ; i++) {
//            Movie m = new Movie();
//            m.setId(""+i);
//            m.setPrimaryTitle("FAKETITLE"+i);
//            m.setStartYear(2000+i);
//            movieList.add(m);
//            System.out.println(movieService.insertMovie(m));
//        }
//    }

}
