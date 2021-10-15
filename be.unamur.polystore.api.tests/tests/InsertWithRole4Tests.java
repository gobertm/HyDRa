package generated;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import dao.impl.*;
import dao.services.*;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import pojo.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.assertEquals;


//import org.apache.spark.sql.Dataset;
import util.Dataset;

public class InsertWithRole4Tests {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InsertWithRole4Tests.class);
    DirectorService directorService = new DirectorServiceImpl();
    Dataset<Director> directorDataset;
    MovieService movieService = new MovieServiceImpl();
    Dataset<Movie> movieDataset;
    ActorService actorService = new ActorServiceImpl();
    Dataset<Actor> actorsDataset;
    ReviewService reviewService = new ReviewServiceImpl();
    Dataset<Review> reviewDataset;
    MovieDirectorService movieDirectorService = new MovieDirectorServiceImpl();
    MovieReviewService movieReviewService = new MovieReviewServiceImpl();
    MovieActorService movieActorService = new MovieActorServiceImpl();
    static List<Actor> actors = new ArrayList<>();
    static List<Movie> movies = new ArrayList<>();
    static List<Director> directors = new ArrayList<>();
    static List<Review> reviews = new ArrayList<>();
    static MongoClient mongoClient;
    static Connection connection;
    static final int NBINSTANCE = 10;

    /*
    To use with code generated based on 'insertR-4.pml'
     */

    @BeforeClass
    public static void setUp() {
        mongoClient = MongoClients.create(
                MongoClientSettings.builder()
                        .applyToClusterSettings(builder ->
                                builder.hosts(Arrays.asList(new ServerAddress("localhost", 27100))))
                        .build());

        try {
            connection = DriverManager.getConnection("jdbc:mysql://" + "localhost" + ":" + 3307 + "/" + "mydb", "root", "password");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < NBINSTANCE ; i++) {
            Movie m = new Movie();
            m.setId(""+i);
            m.setPrimaryTitle("FAKETITLE"+i);
            m.setStartYear(2000+i);
            movies.add(m);
        }
        // Actors
        for (int i = 0; i<NBINSTANCE; i++) {
            Actor a = new Actor();
            a.setId(""+i);
            a.setFullName("fullname"+i);
            a.setYearOfBirth("birth"+i);
            a.setYearOfDeath("death"+i);
            actors.add(a);
        }
        // Directors
        for (int i = 0; i<NBINSTANCE; i++) {
            Director d = new Director();
            d.setId(""+i);
            d.setLastName("lastname"+i);
            d.setFirstName("firstname"+i);
            d.setYearOfBirth(i);
            directors.add(d);
        }
        // Reviews
        for (int i = 0; i < NBINSTANCE; i++) {
            Review r = new Review();
            r.setId("" + i);
            r.setContent("reviewcontent"+i);
            reviews.add(r);
        }
    }

    @Before
    public void truncate() throws SQLException {

        logger.info("START TRUNCATE TABLE AND COLLECTION");
        MongoDatabase mongoDatabase = mongoClient.getDatabase("mymongo");
        mongoDatabase.getCollection("userCol").drop();
        mongoDatabase.getCollection("actorCol").drop();
        mongoDatabase.getCollection("actorCollection").drop();
        mongoDatabase.getCollection("movieCol").drop();
        mongoDatabase.getCollection("accountCol").drop();

        Statement statement = connection.createStatement();
        statement.execute("truncate directorTable");
        statement.execute("truncate directed");
        statement.execute("truncate reviewTable");
        logger.info("TRUNCATE TABLE AND COLLECTION SUCCESSFULLY");
    }

    @Test
    public void testInsertManyToManyJoinTable() {
        // insert movie
        movieService.insertMovie(movies.get(0));
        movieService.insertMovie(movies.get(1));
        movieDataset = movieService.getMovieListInMovieColFromMymongo(null, new MutableBoolean(false));
        assertEquals(2,movieDataset.count());
        // insert director
        directorService.insertDirector(directors.get(0));
        directorService.insertDirector(directors.get(1));
        directorService.insertDirector(directors.get(2));
        directorDataset = directorService.getDirectorListInDirectorTableFromMydb(null, new MutableBoolean(false));
        assertEquals(3,directorDataset.count());

        //JoinTable insertMovieDirector
        movieDirectorService.insertMovieDirector(movies.get(0), directors.get(0));
        assertEquals(1,directorService.getDirectorList(Director.movieDirector.director,movies.get(0)).count());

        // List object
        movieDirectorService.insertMovieDirector(movies.get(1), Arrays.asList(directors.get(1), directors.get(2)));
        directorDataset = directorService.getDirectorList(Director.movieDirector.director, movies.get(1));
        assertEquals(2,directorDataset.count());

        // Test insert REL on non existing entites.
    }

    @Test
    public void insertManyToManyEmbeddedObj() {
        // insert movie
        movieService.insertMovie(movies.get(0));
        movieDataset = movieService.getMovieListInMovieColFromMymongo(null, new MutableBoolean(false));
        assertEquals(1,movieDataset.count());
        // Insert actor
        actorService.insertActor(actors.get(1));

        // Embedded Obj - insertmovieActor
        movieActorService.insertMovieActor(actors.get(1), movies.get(0));
        actorsDataset = actorService.getActorListInActorCollectionFromMymongo(null, new MutableBoolean(false));
        assertEquals(1, actorsDataset.count());
    }

    @Test
    public void insertOneToOneRefStructure(){
        // insert movie
        movieService.insertMovie(movies.get(0));
        movieDataset = movieService.getMovieListInMovieColFromMymongo(null, new MutableBoolean(false));
        assertEquals(1,movieDataset.count());
        // Insert review
        reviewService.insertReview(reviews.get(0), movies.get(0));

//        // Ref Structures - insert movieReview
        assertEquals(movies.get(0).getId(),movieService.getMovie(Movie.movieReview.r_reviewed_movie,reviews.get(0)).getId());
        assertEquals(reviews.get(0).getContent(),reviewService.getReview(Review.movieReview.r_review,movies.get(0)).getContent());
    }
}
