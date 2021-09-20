package generated;

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
import org.apache.spark.sql.Dataset;
import org.bson.Document;
import org.bson.conversions.Bson;
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

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.push;
import static org.junit.Assert.assertEquals;

public class InsertWithRole2Tests {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InsertWithRole2Tests.class);
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
            a.setProfilepic(Byte.parseByte("123"));
            accounts.add(a);
        }
    }

    @Before
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
    public void testUpdateArrayFilter(){
        actorService.insertActor(actors.get(0));
        actorService.insertActor(actors.get(1));
        movieService.insertMovieInActorColFromMymongo(movies.get(0), null, Arrays.asList(actors.get(0)));
        movieService.insertMovieInActorColFromMymongo(movies.get(1), null, Arrays.asList(actors.get(1)));
        // Inserting of Review which is at nested level 3 of collection.
        Bson filter, updateOp;
        filter = new Document();
        //add review to movies array
        Document doc = new Document("idreview","0");
        updateOp = push("movies.$[elem].reviews", doc);
        DBConnectionMgr.upsertInArray(filter, updateOp, Arrays.asList(eq("elem.idmovie", "0")), "actorCol","mymongo");
        // add User to Review element array
        doc = new Document("iduser","0");
        updateOp = push("movies.$[idm].reviews.$[elem].user", doc);
        DBConnectionMgr.upsertInArray(filter, updateOp, Arrays.asList(eq("idm.idmovie","0"),eq("elem.idreview", "0")), "actorCol","mymongo");
    }

    @Test
    public void testInsertFull() {
        // Precondition : Opposite entity types of mandatory roles must already be persisted.
        for (Director d : directors) {
            directorService.insertDirector(d);
        }
        assertEquals(10, directorService.getDirectorList(null).count());
        for (Actor a : actors) {
            actorService.insertActor(a);
        }
        assertEquals(10, actorService.getActorList(null).count());
        int i=0;
        for (User u : users) {
            userService.insertUser(u, accounts.get(i));
            i++;
        }
        i=0;
        for (Movie m : movies) {
            movieService.insertMovie(m, directors.get(i), actors);
            i++;
        }

    }

}
