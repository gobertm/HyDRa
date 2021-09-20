//package generated;
//
//import com.mongodb.MongoClientSettings;
//import com.mongodb.ServerAddress;
//import com.mongodb.client.MongoClient;
//import com.mongodb.client.MongoClients;
//import com.mongodb.client.MongoCollection;
//import com.mongodb.client.MongoDatabase;
//import conditions.*;
//import dao.impl.ActorServiceImpl;
//import dao.impl.DirectorServiceImpl;
//import dao.impl.MovieServiceImpl;
//import dao.impl.ReviewServiceImpl;
//import dao.services.ActorService;
//import dao.services.DirectorService;
//import dao.services.MovieService;
//import dao.services.ReviewService;
//import exceptions.PhysicalStructureException;
//import org.apache.commons.lang.mutable.MutableBoolean;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.RowFactory;
//import org.apache.spark.sql.types.ArrayType;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.types.StructField;
//import org.apache.spark.sql.types.StructType;
//import org.bson.Document;
//import org.bson.conversions.Bson;
//import static com.mongodb.client.model.Filters.eq;
//import static com.mongodb.client.model.Updates.push;
//
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
//import pojo.*;
//
//import java.sql.*;
//import java.util.*;
//
//import static com.mongodb.client.model.Accumulators.addToSet;
//import static org.junit.Assert.assertEquals;
//
//public class InsertWithRoleTests {
//    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InsertWithRoleTests.class);
//    DirectorService directorService = new DirectorServiceImpl();
//    Dataset<Director> directorDataset;
//    MovieService movieService = new MovieServiceImpl();
//    Dataset<Movie> movieDataset;
//    ActorService actorService = new ActorServiceImpl();
//    Dataset<Actor> actorsDataset;
//    ReviewService reviewService = new ReviewServiceImpl();
//    Dataset<Review> reviewDataset;
//    static List<Actor> actors = new ArrayList<>();
//    static List<Movie> movies = new ArrayList<>();
//    static List<Director> directors = new ArrayList<>();
//    static List<Review> reviews = new ArrayList<>();
//    static MongoClient mongoClient;
//    static Connection connection;
//    static final int NBINSTANCE = 10;
//    /*
//    To use with code generated based on 'insertR.pml'
//     */
//
//    @BeforeClass
//    public static void setUp() {
//        mongoClient = MongoClients.create(
//                MongoClientSettings.builder()
//                        .applyToClusterSettings(builder ->
//                                builder.hosts(Arrays.asList(new ServerAddress("localhost", 27100))))
//                        .build());
//
//        try {
//            connection = DriverManager.getConnection("jdbc:mysql://" + "localhost" + ":" + 3307 + "/" + "mydb", "root", "password");
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//
//        for (int i = 0; i < NBINSTANCE ; i++) {
//            Movie m = new Movie();
//            m.setId(""+i);
//            m.setPrimaryTitle("FAKETITLE"+i);
//            m.setStartYear(2000+i);
//            movies.add(m);
//        }
//        // Actors
//        for (int i = 0; i<NBINSTANCE; i++) {
//            Actor a = new Actor();
//            a.setId(""+i);
//            a.setFullName("fullname"+i);
//            a.setYearOfBirth("birth"+i);
//            a.setYearOfDeath("death"+i);
//            actors.add(a);
//        }
//        // Directors
//        for (int i = 0; i<NBINSTANCE; i++) {
//            Director d = new Director();
//            d.setId(""+i);
//            d.setLastName("lastname"+i);
//            d.setFirstName("firstname"+i);
//            d.setYearOfBirth(i);
//            directors.add(d);
//        }
//
//        for (int i = 0; i < NBINSTANCE; i++) {
//            Review r = new Review();
//            r.setId("" + i);
//            r.setContent("reviewcontent"+i);
//            reviews.add(r);
//        }
//    }
//
//    @Before
//    public void truncate() throws SQLException {
//
//        logger.info("START TRUNCATE TABLE AND COLLECTION");
//        MongoDatabase mongoDatabase = mongoClient.getDatabase("mymongo");
//        mongoDatabase.getCollection("actorCollection").drop();
//        mongoDatabase.getCollection("movieCol").drop();
//        mongoDatabase.getCollection("reviewCol").drop();
//
//        Statement statement = connection.createStatement();
//        statement.execute("truncate directorTable");
//        statement.execute("truncate directed");
//        logger.info("TRUNCATE TABLE AND COLLECTION SUCCESSFULLY");
//    }
//
//    @Test
//    public void insertDescendingInDocumentDB(){
//        movieService.insertMovieInMovieColFromMymongo(movies.get(0),directors,actors);
//        movieDataset = movieService.getMovieList(Movie.movieActor.movie, Condition.simple(ActorAttribute.fullName, Operator.EQUALS, "fullname0"));
//        movieDataset.show();
//        assertEquals(1, movieDataset.count());
//        assertEquals("FAKETITLE0", movieDataset.collectAsList().get(0).getPrimaryTitle());
//    }
//
//    @Test
//    public void insertAscendingInDocumentDB(){
//        // Insert Actors first. As Ascending need preexisting actors. Implementation will insert in standalone structure 'actorCollection' (because no mandatory roles on actors)
//        for (Actor a : actors) {
//            actorService.insertActor(a);
//        }
//        assertEquals(10, actorService.getActorList(null).count());
//        //Insert Movie in ascending structure.
//        // To all root documents
//        movieService.insertMovieInActorCollectionFromMymongo(movies.get(0),directors,actors);
//        actorsDataset = actorService.getActorList(Actor.movieActor.character, Condition.simple(MovieAttribute.id, Operator.EQUALS, "0"));
//        actorsDataset.show();
//        assertEquals(NBINSTANCE, actorsDataset.count());
//        // To 2 root documents
//        movieService.insertMovieInActorCollectionFromMymongo(movies.get(1),directors,Arrays.asList(actors.get(0), actors.get(1)));
//        actorsDataset = actorService.getActorList(Actor.movieActor.character, Condition.simple(MovieAttribute.id, Operator.EQUALS, "1"));
//        assertEquals(2, actorsDataset.count());
//    }
//
//    @Test(expected= PhysicalStructureException.class)
//    public void testExceptionWhenEmbeddedMandatoryRoleObjectsAreNotSet(){
//        User author = new User();
//        author.setId("0");
//        author.setCity("NAMUR");
//        movies.get(0)._setCharacterList(null);
//        Condition condition = Condition.simple(ReviewAttribute.id, Operator.EQUALS, "0");
//        reviewService.insertReviewInReviewColFromMymongo(reviews.get(0), movies.get(0), author);
//    }
//
//    @Test
//    public void insertComplexEmbeddedStructureDocumentDB() {
//        User author = new User();
//        author.setId("0");
//        author.setCity("NAMUR");
//        Condition condition = Condition.simple(ReviewAttribute.id, Operator.EQUALS, "0");
//        Movie m = movies.get(0);
//        m._setCharacterList(actors);
//        reviewService.insertReviewInReviewColFromMymongo(reviews.get(0), m, author);
//        reviewDataset = reviewService.getReviewListInReviewColFromMymongo(condition, new MutableBoolean(false));
//        reviewDataset.show();
//        assertEquals(1, reviewDataset.count());
//    }
//
//    @Test
//    public void insertInJoinStructure(){
//        // Precondition : Opposite entity types of mandatory roles must already be persisted.
//        for (Director d : directors) {
//            directorService.insertDirector(d);
//        }
//        //Movies must also exist in 'actorCollection' in order to be able to retrieve Movie object and to satisfy the declared references in 'directed' table.
//        actorService.insertActorInActorCollectionFromMymongo(actors.get(0));
//        movieService.insertMovieInActorCollectionFromMymongo(movies.get(0), directors, List.of(actors.get(0)));
//        Condition condition = Condition.simple(MovieAttribute.id, Operator.EQUALS, "0");
//        movieService.insertMovieInDirectedFromMydb(movies.get(0),directors,actors);
//        directorDataset = directorService.getDirectorList(Director.movieDirector.director,condition);
//        directorDataset.show();
//        assertEquals(10,directorDataset.count());
//    }
//
//    @Test
//    public void testInsertMovieFull() {
//        // Precondition : Opposite entity types of mandatory roles must already be persisted.
//        for (Director d : directors) {
//            directorService.insertDirector(d);
//        }
//        assertEquals(10, directorService.getDirectorList(null).count());
//        for (Actor a : actors) {
//            actorService.insertActor(a);
//        }
//        assertEquals(10, actorService.getActorList(null).count());
//
//        Condition condition = Condition.simple(MovieAttribute.id, Operator.EQUALS, "0");
//        // Insert 1 Movie, gives list of NBINSTANCE of Actors and Directors.
//        movieService.insertMovie(movies.get(0), directors, actors);
//        movieDataset=movieService.getMovieListInMovieColFromMymongo(condition, new MutableBoolean(false));
//        movieDataset.show();
//        assertEquals(1,movieDataset.count());
//        actorsDataset = actorService.getActorList(Actor.movieActor.character, condition);
//        actorsDataset.show();
//        assertEquals(10,actorsDataset.count());
//        directorDataset = directorService.getDirectorList(Director.movieDirector.director,condition);
//        directorDataset.show();
//        assertEquals(10,directorDataset.count());
//    }
//
//}
