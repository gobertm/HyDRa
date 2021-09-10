package generated;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import conditions.ActorAttribute;
import conditions.Condition;
import conditions.MovieAttribute;
import conditions.Operator;
import dao.impl.ActorServiceImpl;
import dao.impl.DirectorServiceImpl;
import dao.impl.MovieServiceImpl;
import dao.services.ActorService;
import dao.services.DirectorService;
import dao.services.MovieService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.push;
import org.junit.BeforeClass;
import org.junit.Test;
import pojo.Actor;
import pojo.Director;
import pojo.Movie;

import java.util.*;

import static com.mongodb.client.model.Accumulators.addToSet;
import static org.junit.Assert.assertEquals;

public class InsertWithRoleTests {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InsertWithRoleTests.class);
    DirectorService directorService = new DirectorServiceImpl();
    Dataset<Director> directorDataset;
    MovieService movieService = new MovieServiceImpl();
    Dataset<Movie> movieDataset;
    ActorService actorService = new ActorServiceImpl();
    Dataset<Actor> actorsDataset;
    static List<Actor> actors = new ArrayList<>();
    static List<Movie> movies = new ArrayList<>();
    static List<Director> directors = new ArrayList<>();
    /*
    To use with code generated based on 'insertR.pml'
     */

    @BeforeClass
    public static void setUp() {
        for (int i = 0; i < 10 ; i++) {
            Movie m = new Movie();
            m.setId(""+i);
            m.setPrimaryTitle("FAKETITLE"+i);
            m.setStartYear(2000+i);
            movies.add(m);
        }
        // Actors
        for (int i = 0; i<5; i++) {
            Actor a = new Actor();
            a.setId(""+i);
            a.setFullName("fullname"+i);
            a.setYearOfBirth("birth"+i);
            a.setYearOfDeath("death"+i);
            actors.add(a);
        }
        // Directors
        for (int i = 0; i<2; i++) {
            Director d = new Director();
            d.setId(""+i);
            d.setLastName("lastname"+i);
            d.setFirstName("firstname"+i);
            d.setYearOfBirth(i);
            directors.add(d);
        }
    }
    @Test
    public void insertDescendingInDocumentDB(){
        movieService.insertMovieInMovieColFromMymongo(movies.get(0),directors,actors);
        movieDataset = movieService.getMovieList(Movie.movieActor.movie, Condition.simple(ActorAttribute.fullName, Operator.EQUALS, "fullname0"));
        movieDataset.show();
        assertEquals(1, movieDataset.count());
        assertEquals("FAKETITLE0", movieDataset.collectAsList().get(0).getPrimaryTitle());
    }

    @Test
    public void insertAscendingInDocumentDB(){
        Movie m = new Movie();
        m.setId("999");
        m.setPrimaryTitle("MOVIETITLE");
        // Insert Actors first. As Ascending need preexisting actors. Implementation will insert in standalone structure 'actorCollection' (because no mandatory roles on actors)
        for (Actor a : actors) {
            actorService.insertActor(a);
        }
        //Insert Movie in ascending structure.
        movieService.insertMovieInActorCollectionFromMymongo(m,directors,actors);
        actorsDataset = actorService.getActorList(Actor.movieActor.character, Condition.simple(MovieAttribute.id, Operator.EQUALS, "999"));
        actorsDataset.show();
        assertEquals(5, actorsDataset.count());
    }

    @Test
    public void testUpsertArrayMongo() {
        MongoClient mongoClient;
        MongoDatabase mongoDatabase;
        mongoClient = MongoClients.create(
                MongoClientSettings.builder()
                        .applyToClusterSettings(builder ->
                                builder.hosts(Arrays.asList(new ServerAddress("localhost", 27100))))
                        .build());

        mongoDatabase = mongoClient.getDatabase("mymongo");
        MongoCollection<Document> actorCollection = mongoDatabase.getCollection("actorCollection");
        Bson filter = eq("id","11");
        Bson updateOp = push("movies", new Document("idmovie", "111").append("title", "FAKETITLE111"));
//        Bson updateOp = addToSet("movies", new Document("idmovie", "111").append("title", "FAKETITLE111"));
        actorCollection.updateMany(filter, updateOp);
    }

}
