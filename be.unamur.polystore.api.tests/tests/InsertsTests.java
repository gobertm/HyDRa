//
//package generated;
//
//import dao.impl.ActorServiceImpl;
//import dao.impl.DirectorServiceImpl;
//import dao.impl.MovieServiceImpl;
//import dao.services.ActorService;
//import dao.services.DirectorService;
//import dao.services.MovieService;
//import org.apache.commons.lang.mutable.MutableBoolean;
//import org.apache.spark.sql.Dataset;
//import org.junit.Test;
//import pojo.Actor;
//import pojo.Director;
//import pojo.Movie;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import static org.junit.Assert.assertEquals;
//
//public class InsertsTests {
//
//    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InsertsTests.class);
//    DirectorService directorService = new DirectorServiceImpl();
//    Dataset<Director> directorDataset;
//    MovieService movieService = new MovieServiceImpl();
//    Dataset<Movie> movieDataset;
//    ActorService actorService = new ActorServiceImpl();
//    Dataset<Actor> actors;
//
//    /*
//    All tests below are to be run on 'tests-imdb.pml' model.
//    With empty databases.
//     */
//    @Test
//    public void testInsertAndGetKeyValue() {
//        // Test with Acceleo code generated from 'imdb_keyvalue.pml'
//        // Redis docker db must be emptied first. 'flushall' command.
//        MovieService movieService = new MovieServiceImpl();
//        for (int i = 0; i < 10 ; i++) {
//            Movie m = new Movie();
//            m.setId(""+i);
//            m.setPrimaryTitle("FAKETITLE"+i);
//            m.setStartYear(2000+i);
//            movieService.insertMovie(m);
////            System.out.println(movieService.insertMovie(m));
//        }
//        assertEquals(10, movieService.getMovieListInMovieKVFromMyredis(null,new MutableBoolean(false)).count());
////        movieService.getMovieList(null).show();
//    }
//
//    @Test
//    public void testInsertAndGetDocumentDB(){
//        ActorService actorService = new ActorServiceImpl();
//        for (int i = 10; i<20; i++) {
//            Actor a = new Actor();
//            a.setId(""+i);
//            a.setFullName("fullname"+i);
//            a.setYearOfBirth("birth"+i);
//            a.setYearOfDeath("death"+i);
//            actorService.insertActor(a);
////            System.out.println(actorService.insertActor(a));
//        }
//        assertEquals(10, actorService.getActorListInActorCollectionFromMymongo(null,new MutableBoolean(false)).count());
////        actorService.getActorList(null).show();
//    }
//
//    @Test
//    public void insertAndGetRelationalDB(){
//        DirectorService directorService = new DirectorServiceImpl();
//        for (int i = 0; i<10; i++) {
//            Director d = new Director();
//            d.setId(""+i);
//            d.setLastName("lastname"+i);
//            d.setFirstName("firstname"+i);
//            d.setYearOfBirth(i);
//            directorService.insertDirector(d);
//        }
//        assertEquals(10, directorService.getDirectorListInDirectorTableFromMydb(null,new MutableBoolean(false)).count());
//    }
//
//}
