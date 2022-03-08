import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import conditions.*;
import dao.impl.*;
import dao.services.*;
import dbconnection.DBConnectionMgr;
import exceptions.PhysicalStructureException;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pojo.*;
import util.Dataset;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.push;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class insertR2Tests {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(insertR2Tests.class);
    DirectorService directorService = new DirectorServiceImpl();
    Dataset<Director> directorDataset;
    MovieService movieService = new MovieServiceImpl();
    Dataset<Movie> movieDataset;
    ActorService actorService = new ActorServiceImpl();
    Dataset<Actor> actorsDataset;
    ReviewService reviewService = new ReviewServiceImpl();
    Dataset<Review> reviewDataset;
    UserService userService = new UserServiceImpl();
    Dataset<User> userDataset;
    AccountService accountService = new AccountServiceImpl();
    Dataset<Account> accountDataset;
    static List<Actor> actors = new ArrayList<>();
    static List<Movie> movies = new ArrayList<>();
    static List<Director> directors = new ArrayList<>();
    static List<Review> reviews = new ArrayList<>();
    static List<Account> accounts = new ArrayList<>();
    static List<User> users = new ArrayList<>();
    static MongoClient mongoClient;
    static Connection connection;
    static final int NBINSTANCE = 10;
    /*
    To use with code generated based on 'insertR-2.pml'
     */

    @BeforeAll
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

        for (int i = 0; i < NBINSTANCE; i++) {
            Review r = new Review();
            r.setId("" + i);
            r.setContent("reviewcontent"+i);
            reviews.add(r);
        }

        for (int i = 0; i < NBINSTANCE; i++) {
            User u = new User();
            u.setId("" + i);
            users.add(u);
        }

        for (int i = 0; i < NBINSTANCE; i++) {
            Account a = new Account();
            a.setId("" + i);
            a.setEmail(i + "_user@fakemail.com");
            accounts.add(a);
        }
    }

    @BeforeEach
    public void truncate() throws SQLException {

        logger.info("START TRUNCATE TABLE AND COLLECTION");
        MongoDatabase mongoDatabase = mongoClient.getDatabase("mymongo");
        mongoDatabase.getCollection("userCol").drop();
        mongoDatabase.getCollection("actorCol").drop();

        Statement statement = connection.createStatement();
        statement.execute("truncate directorTable");
        statement.execute("truncate movieTable");
        logger.info("TRUNCATE TABLE AND COLLECTION SUCCESSFULLY");
    }


    @Test
    public void testInsertAscendingLevel1(){
        actorService.insertActor(actors.get(0));
        actorService.insertActor(actors.get(1));
        // ascending level 1
        // movie id 0 to only actor id 0
        movieService.insertMovieInActorColFromMymongo(movies.get(0), null, Arrays.asList(actors.get(0)));
        assertEquals(1,actorService.getActorList(Actor.movieActor.character, movies.get(0)).count());
        // movie id 1 to actor 0 & 1
        movieService.insertMovieInActorColFromMymongo(movies.get(1), null, Arrays.asList(actors.get(0),actors.get(1)));
        assertEquals(2,actorService.getActorList(Actor.movieActor.character, movies.get(1)).count());
    }

    @Test
    public void testInsertAscendingLevel2OnlyOneDocLevel1(){
        actorService.insertActor(actors.get(0));
        actorService.insertActor(actors.get(1));
        // ascending level 1
        // movie id 0 to only actor id 0
        movieService.insertMovieInActorColFromMymongo(movies.get(0), null, Arrays.asList(actors.get(0)));
        // movie id 1 to actor 0 & 1
        movieService.insertMovieInActorColFromMymongo(movies.get(1), null, Arrays.asList(actors.get(0),actors.get(1)));
        assertEquals(2,movieService.getMovieListInActorColFromMymongo(null,new MutableBoolean(false)).count());
        // ascending level 2
        // insert review 0 to movie 0 , therefore actor 0 only
        reviewService.insertReviewInActorColFromMymongo(reviews.get(0), movies.get(0), users.get(0));
        reviewDataset = reviewService.getReviewListInActorColFromMymongo(null,new MutableBoolean(false));
        assertEquals(1,reviewDataset.count());
        actorsDataset = actorService.getActorList(Actor.movieActor.character, movieService.getMovie(Movie.movieReview.r_reviewed_movie, reviewDataset.collectAsList().get(0)));
        assertEquals(1,actorsDataset.count());
    }

    @Test
    public void testInsertAscendingLevel2MultiDocLevel1(){
        actorService.insertActor(actors.get(0));
        actorService.insertActor(actors.get(1));
        // ascending level 1
        // movie id 0 to only actor id 0
        movieService.insertMovieInActorColFromMymongo(movies.get(0), null, Arrays.asList(actors.get(0)));
        // movie id 1 to actor 0 & 1
        movieService.insertMovieInActorColFromMymongo(movies.get(1), null, Arrays.asList(actors.get(0),actors.get(1)));
        assertEquals(2,movieService.getMovieListInActorColFromMymongo(null,new MutableBoolean(false)).count());
        // ascending level 2
        // Insert review 1 to movie 1 , so to both actor 0 & 1
        reviewService.insertReviewInActorColFromMymongo(reviews.get(1), movies.get(1), users.get(1));
        actorsDataset = actorService.getActorList(Actor.movieActor.character, movieService.getMovie(Movie.movieReview.r_reviewed_movie, reviews.get(1)));
        assertEquals(2,actorsDataset.count());
    }

    @Test
    public void testInsertFull() {
        // Precondition : Opposite entity types of mandatory roles must already be persisted.
        for (Director d : directors) {
            directorService.insertDirector(d);
        }
        assertEquals(NBINSTANCE, directorService.getDirectorList(null).count());
        assertEquals(NBINSTANCE, directorService.getDirectorListInDirectorTableFromMydb(null, new MutableBoolean(false)).count());
        for (Actor a : actors) {
            actorService.insertActor(a);
        }
        assertEquals(NBINSTANCE, actorService.getActorList(null).count());
        int i=0;
        for (User u : users) {
            userService.insertUser(u, accounts.get(i));
            i++;
        }
        assertEquals(NBINSTANCE, accountService.getAccountList(null).count());
        assertEquals(NBINSTANCE, userService.getUserListInUserColFromMymongo(null, new MutableBoolean(false)).count());
        // Users are not inserted in 'actorCol' because it relies on Review embedded structure, and Review is not a mandaotry role.
        i=0;
        for (Movie m : movies) {
            movieService.insertMovie(m, directors.get(i), actors);
            i++;
        }
        assertEquals(NBINSTANCE, movieService.getMovieList(null).count());
        assertEquals(NBINSTANCE, movieService.getMovieListInActorColFromMymongo(null, new MutableBoolean(false)).count());
        assertEquals(NBINSTANCE, movieService.getMovieListInMovieTableFromMydb(null, new MutableBoolean(false)).count());
        assertEquals(NBINSTANCE, movieService.getMovieList(Movie.movieActor.movie,actors.get(0)).count());
        assertEquals(directors.get(0).getId(), directorService.getDirector(Director.movieDirector.director, movies.get(0)).getId());
        assertEquals(1, movieService.getMovieList(Movie.movieDirector.directed_movie, directors.get(0)).count());
    }

}
