package generated;

import conditions.*;
import dao.impl.ActorServiceImpl;
import dao.impl.DirectorServiceImpl;
import dao.impl.MovieServiceImpl;
import dao.services.ActorService;
import dao.services.DirectorService;
import dao.services.MovieService;
import dbconnection.SparkConnectionMgr;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import pojo.Actor;
import pojo.Director;
import pojo.Movie;
import pojo.MovieDirector;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KeyValueTests {

    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KeyValueTests.class);
    DirectorService directorService = new DirectorServiceImpl();
    Dataset<Director> directorDataset;
    MovieService movieService = new MovieServiceImpl();
    Dataset<Movie> movieDataset;
    ActorService actorService = new ActorServiceImpl();
    Dataset<Actor> actors;

//    @Test
//    public void testInsertAndGetKeyValue() {
//        // Test with Acceleo code generated from 'imdb_keyvalue.pml'
//        // Redis docker db must be emptied first. 'flushall' command.
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
//        assertEquals(10, movieService.getMovieListInMovieKVFromMyredis(null,new MutableBoolean(false)).count());
//        assertEquals(10, movieService.getMovieListInMovieKV2FromMyredis(null,new MutableBoolean(false)).count());
//        assertTrue(movieService.getMovieListInMovieKVemptyFromMyredis(null,new MutableBoolean(false)).isEmpty());
//        movieService.getMovieList(null).show();
//    }
//
//    @Test
//    public void testRefilterFlagKeyValue(){
//        Dataset<Movie> movies;
//        MutableBoolean mutableBool = new MutableBoolean(false);
//        Condition<MovieAttribute> idcond = SimpleCondition.simple(MovieAttribute.id, Operator.EQUALS, "0");
//        Condition<MovieAttribute> titlecond = SimpleCondition.simple(MovieAttribute.primaryTitle, Operator.EQUALS, "FAKETITLE7");
//        OrCondition orCondition = OrCondition.or(idcond, titlecond);
//        movies = movieService.getMovieListInMovieKVFromMyredis(orCondition,mutableBool);
//        assertEquals(true,mutableBool.booleanValue());
//        movies = movieService.getMovieListInMovieKVFromMyredis(idcond,mutableBool);
//        mutableBool.setValue(false);
//        assertEquals(false,mutableBool.booleanValue());
//        mutableBool.setValue(false);
//        movies = movieService.getMovieListInMovieKVFromMyredis(titlecond,mutableBool);
//        assertEquals(true,mutableBool.booleanValue());
//    }

    @Test
    public void testGetListInKeyValue() {
//        Jedis jedis = new Jedis("localhost", 6379);
//        for (int i = 1; i < 10; i++) {
//            jedis.lpush("movie:"+i+":rates", ""+i,"8", "2", "9", "10");
//        }
//        jedis.lpush("movie:1:rates", "1", "2", "3", "4");
//        jedis.lpush("movie:2:rates", "5", "6", "7", "8");
//        jedis.lpush("movie:3:rates", "9", "10", "11", "12");
//        Dataset<Row> rows = SparkConnectionMgr.getRowsFromKeyValue("myredis", "movie:*:rates");
//        rows.show();
//        Dataset<Row> r = SparkConnectionMgr.getRowsFromKeyValueLists("myredis", "movie:*:rates", null);
//        r.show();
//        for (int i = 1; i < 10; i++) {
//            jedis.del("movie:"+i+":rates");
//        }
    }

    @Test
    public void testGetListRoleIDInKeyValue() {
        Jedis jedis = new Jedis("localhost", 6379);
        for (int i = 1; i < 10; i++) {
            jedis.lpush("actor:"+i+":movies",""+i+2,""+i);
        }
    }

}
