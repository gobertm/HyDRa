//package generated;
//
//import com.mongodb.MongoClientSettings;
//import com.mongodb.ServerAddress;
//import com.mongodb.client.MongoClient;
//import com.mongodb.client.MongoClients;
//import com.mongodb.client.MongoDatabase;
//import conditions.ReviewAttribute;
//import conditions.SimpleCondition;
//import dao.impl.CityServiceImpl;
//import dao.impl.ReviewServiceImpl;
//import dao.impl.UserServiceImpl;
//import dao.services.CityService;
//import dao.services.CityUserService;
//import dao.services.ReviewService;
//import dao.services.UserService;
//import org.apache.commons.lang3.RandomStringUtils;
//import org.apache.commons.lang3.RandomUtils;
//import org.apache.spark.sql.Dataset;
//import org.bson.Document;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
//import pojo.*;
//import redis.clients.jedis.Jedis;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//import java.sql.Statement;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertNotNull;
//
//public class DocumentDesignstTests {
//    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DocumentDesignstTests.class);
//    private static ReviewService reviewService = new ReviewServiceImpl();
//    private static CityService cityService = new CityServiceImpl();
//    private static UserService userService = new UserServiceImpl();
//    private static SimpleCondition<ReviewAttribute> reviewCondition;
//    private final static int NBINSTANCE = 20;
//    private static List<Movie> movies = new ArrayList<>();
//    private static List<Review> reviews = new ArrayList<>();
//    private static List<User> users = new ArrayList<>();
//    private static List<City> cities = new ArrayList<>();
//    private static List<Account> accounts = new ArrayList<>();
//    private static List<Document> docCityCol = new ArrayList<>();
//    private Dataset<Review> resReview;
//    private Dataset<City> resCity;
//    private Dataset<User> resUser;
//    static MongoClient mongoClient;
//    static Connection connection;
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
//        // Movies
//        for (int i = 0; i < NBINSTANCE ; i++) {
//            Movie m = new Movie();
//            m.setId(""+i);
//            m.setPrimaryTitle("FAKETITLE"+i);
//            m.setStartYear(2000+i);
//            movies.add(m);
//        }
//        // Reviews
//        for (int i = 0; i < NBINSTANCE; i++) {
//            Review r = new Review();
//            r.setId("" + i);
//            r.setContent("reviewcontent"+i);
//            reviews.add(r);
//        }
//        // Account
//        for (int i = 0; i < NBINSTANCE; i++) {
//            Account a = new Account();
//            a.setEmail("EMAILUser"+i+"@email.com");
//            a.setId("account"+i);
//            accounts.add(a);
//        }
//        // User
//        for (int i = 0; i < NBINSTANCE; i++) {
//            User u = new User();
//            u.setId("user" + i);
//            users.add(u);
//        }
//        // City
//        City cp = new City();
//        cp.setName("CITY_PAIR");
//        cities.add(cp);
//        City co = new City();
//        co.setName("CITY_ODD");
//        cities.add(co);
//
//        // User Citizen
//        Document docPair = new Document("cityName","CITY_PAIR");
//        Document docOdd = new Document("cityName", "CITY_ODD");
//        List refpair = new ArrayList();
//        List refodd = new ArrayList();
//        int j = 0;
//        for (User u : users) {
//            if(j%2==0)
//                refpair.add("user" + j);
//            else
//                refodd.add("user"+j);
//            j++;
//        }
//        docPair.append("citizens", refpair);
//        docOdd.append("citizens", refodd);
//        docCityCol.add(docPair);
//        docCityCol.add(docOdd);
//    }
//
//    @Before
//    public void truncate() throws SQLException {
//
//        logger.info("START TRUNCATE TABLE AND COLLECTION");
//        MongoDatabase mongoDatabase = mongoClient.getDatabase("mymongo");
//        mongoDatabase.getCollection("userCol").drop();
//        mongoDatabase.getCollection("actorCol").drop();
//        mongoDatabase.getCollection("reviewCol").drop();
//        mongoDatabase.getCollection("movieCol").drop();
//        mongoDatabase.getCollection("cityCol").drop();
//
//        Statement statement = connection.createStatement();
//        statement.execute("truncate directorTable");
//        statement.execute("truncate movieTable");
//        logger.info("TRUNCATE TABLE AND COLLECTION SUCCESSFULLY");
//
//        // Add Data
//        mongoDatabase.getCollection("cityCol").insertMany(docCityCol);
//        int i=0;
//        for (User u : users) {
//            City c;
//            if (i % 2 == 0) {
//                c = cities.get(0);
//            } else {
//                c = cities.get(1);
//            }
//            userService.insertUser(u, accounts.get(i), c);
//            i++;
//        }
//    }
//
//    @Test
//    public void testGetRefInArrayDocDB(){
//        // Get Users by City
//        resUser = userService.getUserList(User.cityUser.inhabitants, cities.get(0));
//        resUser.show();
//        assertEquals(10, resUser.count());
//
//        // Get City by user
//        City city = cityService.getCity(City.cityUser.resident_town, users.get(1));
//        assertNotNull(city);
//        System.out.println(city);
//    }
//}
