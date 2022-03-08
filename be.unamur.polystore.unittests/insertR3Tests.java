import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import conditions.Condition;
import conditions.Operator;
import conditions.ReviewAttribute;
import conditions.UserAttribute;
import dao.impl.*;
import dao.services.*;
import exceptions.PhysicalStructureException;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import pojo.*;
import util.Dataset;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class insertR3Tests {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(insertR3Tests.class);
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
	 * To use with code generated based on 'insertR-3.pml'
	 */

	@BeforeAll
	public static void setUp() {
		mongoClient = MongoClients.create(MongoClientSettings.builder()
				.applyToClusterSettings(builder -> builder.hosts(Arrays.asList(new ServerAddress("localhost", 27100))))
				.build());

		try {
			connection = DriverManager.getConnection("jdbc:mysql://" + "localhost" + ":" + 3307 + "/" + "mydb", "root",
					"password");
		} catch (SQLException e) {
			e.printStackTrace();
		}

		for (int i = 0; i < NBINSTANCE; i++) {
			Movie m = new Movie();
			m.setId("" + i);
			m.setPrimaryTitle("FAKETITLE" + i);
			m.setStartYear(2000 + i);
			movies.add(m);
		}
		// Actors
		for (int i = 0; i < NBINSTANCE; i++) {
			Actor a = new Actor();
			a.setId("" + i);
			a.setFullName("fullname" + i);
			a.setYearOfBirth("birth" + i);
			a.setYearOfDeath("death" + i);
			actors.add(a);
		}
		// Directors
		for (int i = 0; i < NBINSTANCE; i++) {
			Director d = new Director();
			d.setId("" + i);
			d.setLastName("lastname" + i);
			d.setFirstName("firstname" + i);
			d.setYearOfBirth(i);
			directors.add(d);
		}

		for (int i = 0; i < NBINSTANCE; i++) {
			Review r = new Review();
			r.setId("" + i);
			r.setContent("reviewcontent" + i);
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
		mongoDatabase.getCollection("reviewCol").drop();
		mongoDatabase.getCollection("movieCol").drop();

		Statement statement = connection.createStatement();
		statement.execute("truncate directorTable");
		statement.execute("truncate movieTable");
		logger.info("TRUNCATE TABLE AND COLLECTION SUCCESSFULLY");
	}

	@Test
	public void testInsertInDescendingStructWithRef() {
		// preexisting movie. Physical structures of insertR-3 do not require Director
		// and Actor
		movieService.insertMovie(movies.get(0), null, null);
		reviewService.insertReview(reviews.get(0), movies.get(0), users.get(0));
		reviewDataset = reviewService.getReviewListInReviewColFromMymongo(
				Condition.simple(ReviewAttribute.id, Operator.EQUALS, "0"), new MutableBoolean(false));
		Movie m = movieService.getMovie(Movie.movieReview.r_reviewed_movie, reviewDataset.first());
		assertEquals(m.getPrimaryTitle(), movies.get(0).getPrimaryTitle());
	}

	@Test
	public void testInsertInAscendingStructWithRef() {
		// preexisting movie. Physical structures of insertR-3 do not require Director
		// and Actor
		movieService.insertMovie(movies.get(0), null, null);
		// Ref account is in embedded User object.
		users.get(0)._setAccount(accounts.get(0));
		// Account data must already exist in db. So we insert
		accountService.insertAccount(accounts.get(0), users.get(0));

		// insert Review
		reviewService.insertReview(reviews.get(0), movies.get(0), users.get(0));
		userDataset = userService.getUserListInReviewColFromMymongo(
				Condition.simple(UserAttribute.id, Operator.EQUALS, "0"), new MutableBoolean(false));
		accountDataset = accountService.getAccountList(Account.userAccount.account,
				Condition.simple(UserAttribute.id, Operator.EQUALS, "0"));
		assertEquals(accounts.get(0).getEmail(), accountDataset.collectAsList().get(0).getEmail());
	}

	@Test
	public void testInsertExceptionInAscendingStructWithRef() {
		Assertions.assertThrows(PhysicalStructureException.class, () -> {
			users.get(0)._setAccount(null);
			// preexisting movie. Physical structures of insertR-3 do not require Director
			// and Actor
			movieService.insertMovie(movies.get(0), null, null);
			reviewService.insertReview(reviews.get(0), movies.get(0), users.get(0));
		});
	}

}
