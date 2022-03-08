import conditions.Operator;
import conditions.OrderAttribute;
import conditions.SimpleCondition;
import dao.impl.OrderServiceImpl;
import dao.services.OrderService;

import pojo.Order;
import redis.clients.jedis.Jedis;
import util.Dataset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

/*Conceptual API tests on model CS1.*/
public class CS1PSbRelTests {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CS1PSbRelTests.class);
	static final org.slf4j.Logger loggerPerf = org.slf4j.LoggerFactory.getLogger("LogPerf");
	
	//cannot exceed 31 because of the date
	static final int NBINSTANCE = 20;
	static int orderId = 10;
	static MongoClient mongoClient;
	static Connection connection;
	static Jedis jedis;

	static Map<String, Order> orders = new HashMap<String, Order>();

	OrderService orderService = new OrderServiceImpl();
	Dataset<Order> orderDataset;
	LocalDateTime start, end;

	@BeforeAll
	public static void setUp() throws Exception {
		mongoClient = MongoClients.create(MongoClientSettings.builder()
				.applyToClusterSettings(builder -> builder.hosts(Arrays.asList(new ServerAddress("localhost", 27701))))
				.build());
		try {
			connection = DriverManager.getConnection("jdbc:mysql://" + "localhost" + ":" + 3334 + "/" + "mysqlPerfTest",
					"root", "password");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
		jedis = new Jedis("localhost", 6364);

	}
	
	@AfterAll
	public static void closeConnection() throws Exception {
		mongoClient.close();
		connection.close();
		jedis.close();
	}

	@BeforeEach
	public void truncate() throws SQLException {
		try {
			orders = new HashMap<String, Order>();
			for (int i = 1; i <= NBINSTANCE; i++) {
				Order o = new Order();
				o.setId("" + i);
				o.setTotalprice(Double.valueOf(i + "d"));
				String date = "2022-08-" + (i < 10 ? "0" + i : i);
				LocalDate localDate = LocalDate.parse(date);
				o.setOrderdate(localDate);
				orders.put("" + i, o);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		logger.info("START TRUNCATE TABLES, COLLECTIONS AND KVPAIRS");
		MongoDatabase mongoDatabase = mongoClient.getDatabase("mongoPerfTest");
		mongoDatabase.listCollectionNames().forEach(collName -> {
			mongoDatabase.getCollection(collName).drop();
		});

		Statement statement = connection.createStatement();

		ResultSet rs = statement.executeQuery("Show tables");
		Set<String> tables = new HashSet<String>();
		while (rs.next()) {
			tables.add(rs.getString(1));
		}
		rs.close();
		for (String tableName : tables)
			statement.execute("truncate " + tableName);

		statement.close();
		
		jedis.flushAll();
		logger.info("TRUNCATE TABLES COLLECTIONS and KVPAIRS SUCCESSFULLY");

		for (Order o : orders.values())
			orderService.insertOrder(o);
	}

	@Test
	public void testGetEntity() {
		start = LocalDateTime.now();
		loggerPerf.info("Start getEList() [Order] at [{}]", start.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
		orderDataset = orderService.getOrderList();
		assertEquals(NBINSTANCE, orderDataset.count());
		end = LocalDateTime.now();
		loggerPerf.info("End getEList() at [{}]. Duration [{}]m [{}]s [{}]ms.", end,
				Duration.between(start, end).toMinutesPart(), Duration.between(start, end).toSecondsPart(),
				Duration.between(start, end).toMillisPart());

		for (Order o : orderDataset.collectAsList()) {
			String id = o.getId();
			Order o2 = orders.get(id);
			assertNotNull(o);
			assertNotNull(o2);
			assertEquals(o.getId(), o2.getId());
			assertEquals(o.getTotalprice(), o2.getTotalprice());
			assertEquals(o.getOrderdate(), o2.getOrderdate());

			orders.remove(id);
		}

		assertEquals(orders.size(), 0);
	}

	@Test
	public void testGetEByID() {
		
		start = LocalDateTime.now();
		SimpleCondition<OrderAttribute> orderIDCond = SimpleCondition.simple(OrderAttribute.id, Operator.EQUALS,orderId + "");
		loggerPerf.info("Start getEByID() [Order] at [{}]", start);
		orderDataset = orderService.getOrderList(orderIDCond);
		assertEquals(1, orderDataset.count());
		end = LocalDateTime.now();
		loggerPerf.info("End getEByID() at [{}]. Duration [{}]m [{}]s [{}]ms.", end,
				Duration.between(start, end).toMinutesPart(), Duration.between(start, end).toSecondsPart(),
				Duration.between(start, end).toMillisPart());
		assertEquals(orderDataset.first().getOrderdate().toString(), "2022-08-" + (orderId < 10 ? "0" + orderId : orderId));
		assertEquals(orderDataset.first().getTotalprice(), Double.valueOf(orderId + "d"));
	}

	@Test
	public void testGetByAttr() {
		start = LocalDateTime.now();
		SimpleCondition<OrderAttribute> orderDateCond = SimpleCondition.simple(OrderAttribute.orderdate, Operator.GT,
				LocalDate.of(2022, 8, orderId));
		loggerPerf.info("Start getEByAttr(condition) [Order] at [{}]", start);
		orderDataset = orderService.getOrderList(orderDateCond);
		assertEquals(NBINSTANCE - orderId, orderDataset.count());
		end = LocalDateTime.now();
		loggerPerf.info("End getEByAttr(condition) at [{}]. Duration [{}]m [{}]s [{}]ms.", end,
				Duration.between(start, end).toMinutesPart(), Duration.between(start, end).toSecondsPart(),
				Duration.between(start, end).toMillisPart());
		
		
		for(int i = 1; i <= orderId; i++)
			orders.remove(i + "");
		
		for (Order o : orderDataset.collectAsList()) {
			String id = o.getId();
			Order o2 = orders.get(id);
			assertNotNull(o);
			assertNotNull(o2);
			assertEquals(o.getId(), o2.getId());
			assertEquals(o.getTotalprice(), o2.getTotalprice());
			assertEquals(o.getOrderdate(), o2.getOrderdate());

			orders.remove(id);
		}

		assertEquals(orders.size(), 0);
	}

}
