import dao.impl.BuysServiceImpl;
import dao.impl.CustomerServiceImpl;
import dao.impl.OrderServiceImpl;
import dao.services.BuysService;
import dao.services.CustomerService;
import dao.services.OrderService;
import pojo.Buys;
import pojo.Customer;
import pojo.Order;
import redis.clients.jedis.Jedis;
import util.Dataset;
//import org.apache.spark.sql.Dataset;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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

import conditions.Condition;
import conditions.CustomerAttribute;
import conditions.Operator;
import conditions.OrderAttribute;
import conditions.SimpleCondition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

//Conceptual tests on CS2 model. Extends CS1 Tests as those are still valid.
public class CS2PSbDocKeyTests {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CS2PSbDocKeyTests.class);
	static final org.slf4j.Logger loggerPerf = org.slf4j.LoggerFactory.getLogger("LogPerf");
	private Dataset<Customer> customerDataset;
	private CustomerService customerService = new CustomerServiceImpl();
	public BuysService buysService = new BuysServiceImpl();
	public Dataset<Buys> buysDataset;

	// cannot exceed 15 because of the date and must be even
	static final int NBINSTANCEORDERS = 20;
	static final int NBINSTANCECUSTOMERS = NBINSTANCEORDERS / 2;
	static int orderId = 10;
	static int customerId = 5;
	static MongoClient mongoClient;
	static Connection connection;
	static Jedis jedis;

	static Map<String, Order> orders = new HashMap<String, Order>();
	static Map<String, Customer> customers = new HashMap<String, Customer>();

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
			customers = new HashMap<String, Customer>();
			for (int i = 1; i <= NBINSTANCECUSTOMERS; i++) {
				Customer c = new Customer();
				c.setId("" + i);
				c.setFirstname("firstName" + i);
				c.setLastname("lastName" + i);
				c.setBrowser("browser" + i);
				c.setGender("gender" + i);
				c.setLocationip("ip" + i);
				String date = "2022-08-" + (i < 10 ? "0" + i : i);
				LocalDate localDate = LocalDate.parse(date);
				c.setBirthday(localDate);
				c.setCreationDate(localDate);
				customers.put("" + i, c);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			orders = new HashMap<String, Order>();
			for (int i = 1; i <= NBINSTANCEORDERS; i++) {
				Order o = new Order();
				o.setId("" + i);
				o.setTotalprice(Double.valueOf(i + "d"));
				String date = "2022-08-" + (i < 10 ? "0" + i : i);
				LocalDate localDate = LocalDate.parse(date);
				o.setOrderdate(localDate);
				Customer c = customers.get("" + ((Double) Math.ceil((double) i / 2)).intValue());
				System.out.println(c);
				o._setClient(c);
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

		for (Customer c : customers.values())
			customerService.insertCustomer(c);

		for (Order o : orders.values())
			orderService.insertOrder(o, o._getClient());
	}

	@Test
	public void testGetOrderEntity() {
		start = LocalDateTime.now();
		loggerPerf.info("Start getEList() [Order] at [{}]", start.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
		orderDataset = orderService.getOrderList();
		assertEquals(NBINSTANCEORDERS, orderDataset.count());
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
	public void testGetOrderByID() {

		start = LocalDateTime.now();
		SimpleCondition<OrderAttribute> orderIDCond = SimpleCondition.simple(OrderAttribute.id, Operator.EQUALS,
				orderId + "");
		loggerPerf.info("Start getEByID() [Order] at [{}]", start);
		orderDataset = orderService.getOrderList(orderIDCond);
		assertEquals(1, orderDataset.count());
		end = LocalDateTime.now();
		loggerPerf.info("End getEByID() at [{}]. Duration [{}]m [{}]s [{}]ms.", end,
				Duration.between(start, end).toMinutesPart(), Duration.between(start, end).toSecondsPart(),
				Duration.between(start, end).toMillisPart());
		assertEquals(orderDataset.first().getOrderdate().toString(),
				"2022-08-" + (orderId < 10 ? "0" + orderId : orderId));
		assertEquals(orderDataset.first().getTotalprice(), Double.valueOf(orderId + "d"));
	}

	@Test
	public void testGetOrderByAttr() {
		start = LocalDateTime.now();
		SimpleCondition<OrderAttribute> orderDateCond = SimpleCondition.simple(OrderAttribute.orderdate, Operator.GT,
				LocalDate.of(2022, 8, orderId));
		loggerPerf.info("Start getEByAttr(condition) [Order] at [{}]", start);
		orderDataset = orderService.getOrderList(orderDateCond);
		assertEquals(NBINSTANCEORDERS - orderId, orderDataset.count());
		end = LocalDateTime.now();
		loggerPerf.info("End getEByAttr(condition) at [{}]. Duration [{}]m [{}]s [{}]ms.", end,
				Duration.between(start, end).toMinutesPart(), Duration.between(start, end).toSecondsPart(),
				Duration.between(start, end).toMillisPart());

		for (int i = 1; i <= orderId; i++)
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

	@Test
	public void testGetCustomerEntity() {
		start = LocalDateTime.now();
		loggerPerf.info("Start getEList() [Customer] at [{}]", start.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
		customerDataset = customerService.getCustomerList();
		assertEquals(NBINSTANCECUSTOMERS, customerDataset.count());
		end = LocalDateTime.now();
		loggerPerf.info("End getEList() at [{}]. Duration [{}]m [{}]s [{}]ms.", end,
				Duration.between(start, end).toMinutesPart(), Duration.between(start, end).toSecondsPart(),
				Duration.between(start, end).toMillisPart());

		for (Customer o : customerDataset.collectAsList()) {
			String id = o.getId();
			Customer o2 = customers.get(id);
			assertNotNull(o);
			assertNotNull(o2);
			assertEquals(o.getId(), o2.getId());
			assertEquals(o.getBirthday(), o2.getBirthday());
			assertEquals(o.getBrowser(), o2.getBrowser());
			assertEquals(o.getCreationDate(), o2.getCreationDate());
			assertEquals(o.getFirstname(), o2.getFirstname());
			assertEquals(o.getLastname(), o2.getLastname());
			assertEquals(o.getFirstname(), o2.getFirstname());
			assertEquals(o.getGender(), o2.getGender());
			assertEquals(o.getLocationip(), o2.getLocationip());

			customers.remove(id);
		}

		assertEquals(customers.size(), 0);
	}

	@Test
	public void testGetCustomerByID() {

		start = LocalDateTime.now();
		SimpleCondition<CustomerAttribute> custIDCond = SimpleCondition.simple(CustomerAttribute.id, Operator.EQUALS,
				customerId + "");
		loggerPerf.info("Start getEByID() [Customer] at [{}]", start);
		customerDataset = customerService.getCustomerList(custIDCond);
		assertEquals(1, customerDataset.count());
		end = LocalDateTime.now();
		loggerPerf.info("End getEByID() at [{}]. Duration [{}]m [{}]s [{}]ms.", end,
				Duration.between(start, end).toMinutesPart(), Duration.between(start, end).toSecondsPart(),
				Duration.between(start, end).toMillisPart());

		Customer expectedCustomer = customers.get(customerId + "");
		Customer retrievedCustomer = customerDataset.first();
		assertNotNull(expectedCustomer);
		assertNotNull(retrievedCustomer);
		assertEquals(expectedCustomer.getId(), retrievedCustomer.getId());
		assertEquals(expectedCustomer.getBirthday(), retrievedCustomer.getBirthday());
		assertEquals(expectedCustomer.getBrowser(), retrievedCustomer.getBrowser());
		assertEquals(expectedCustomer.getCreationDate(), retrievedCustomer.getCreationDate());
		assertEquals(expectedCustomer.getFirstname(), retrievedCustomer.getFirstname());
		assertEquals(expectedCustomer.getLastname(), retrievedCustomer.getLastname());
		assertEquals(expectedCustomer.getFirstname(), retrievedCustomer.getFirstname());
		assertEquals(expectedCustomer.getGender(), retrievedCustomer.getGender());
		assertEquals(expectedCustomer.getLocationip(), retrievedCustomer.getLocationip());
	}

	@Test
	public void testGetCustomerByAttr() {
		start = LocalDateTime.now();
		SimpleCondition<CustomerAttribute> custDateCond = SimpleCondition.simple(CustomerAttribute.creationDate, Operator.GT,
				LocalDate.of(2022, 8, customerId));
		loggerPerf.info("Start getEByAttr(condition) [Customer] at [{}]", start);
		customerDataset = customerService.getCustomerList(custDateCond);
		assertEquals(NBINSTANCECUSTOMERS - customerId, customerDataset.count());
		end = LocalDateTime.now();
		loggerPerf.info("End getEByAttr(condition) at [{}]. Duration [{}]m [{}]s [{}]ms.", end,
				Duration.between(start, end).toMinutesPart(), Duration.between(start, end).toSecondsPart(),
				Duration.between(start, end).toMillisPart());

		for (int i = 1; i <= customerId; i++)
			customers.remove(i + "");

		for (Customer o : customerDataset.collectAsList()) {
			String id = o.getId();
			Customer o2 = customers.get(id);
			assertNotNull(o);
			assertNotNull(o2);
			assertEquals(o.getId(), o2.getId());
			assertEquals(o.getBirthday(), o2.getBirthday());
			assertEquals(o.getBrowser(), o2.getBrowser());
			assertEquals(o.getCreationDate(), o2.getCreationDate());
			assertEquals(o.getFirstname(), o2.getFirstname());
			assertEquals(o.getLastname(), o2.getLastname());
			assertEquals(o.getFirstname(), o2.getFirstname());
			assertEquals(o.getGender(), o2.getGender());
			assertEquals(o.getLocationip(), o2.getLocationip());
			
			customers.remove(id);
		}

		assertEquals(customers.size(), 0);
	}

	@Test
	public void testGetCustomerByOrderInBuys() {
		start = LocalDateTime.now();
		Order order = orderService.getOrderById(orderId + "");
		int custId = ((Double) Math.ceil((double) orderId / 2)).intValue();
		Customer expectedCustomer = customerService.getCustomerById("" + custId);
		Customer retrievedCustomer;
		loggerPerf.info("Start getEListByRole() [getCustomer(Customer.buys.client, order)] at [{}]",
				start.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
		retrievedCustomer = customerService.getCustomer(Customer.buys.client, order); // Role method
		assertEquals(expectedCustomer.getId(), "" + custId);
		String date = "2022-08-" + (custId < 10 ? "0" + custId : custId);
		LocalDate localDate = LocalDate.parse(date);

		assertEquals(expectedCustomer.getId(), retrievedCustomer.getId());
		assertEquals(expectedCustomer.getBirthday(), retrievedCustomer.getBirthday());
		assertEquals(expectedCustomer.getBrowser(), retrievedCustomer.getBrowser());
		assertEquals(expectedCustomer.getCreationDate(), retrievedCustomer.getCreationDate());
		assertEquals(expectedCustomer.getFirstname(), retrievedCustomer.getFirstname());
		assertEquals(expectedCustomer.getLastname(), retrievedCustomer.getLastname());
		assertEquals(expectedCustomer.getFirstname(), retrievedCustomer.getFirstname());
		assertEquals(expectedCustomer.getGender(), retrievedCustomer.getGender());
		assertEquals(expectedCustomer.getLocationip(), retrievedCustomer.getLocationip());
		end = LocalDateTime.now();

		expectedCustomer = customers.get(custId + "");
		assertEquals(expectedCustomer.getId(), retrievedCustomer.getId());
		assertEquals(expectedCustomer.getBirthday(), retrievedCustomer.getBirthday());
		assertEquals(expectedCustomer.getBrowser(), retrievedCustomer.getBrowser());
		assertEquals(expectedCustomer.getCreationDate(), retrievedCustomer.getCreationDate());
		assertEquals(expectedCustomer.getFirstname(), retrievedCustomer.getFirstname());
		assertEquals(expectedCustomer.getLastname(), retrievedCustomer.getLastname());
		assertEquals(expectedCustomer.getFirstname(), retrievedCustomer.getFirstname());
		assertEquals(expectedCustomer.getGender(), retrievedCustomer.getGender());
		assertEquals(expectedCustomer.getLocationip(), retrievedCustomer.getLocationip());

		loggerPerf.info("End getEListByRole() at [{}]. Duration [{}]m [{}]s [{}]ms.", end,
				Duration.between(start, end).toMinutesPart(), Duration.between(start, end).toSecondsPart(),
				Duration.between(start, end).toMillisPart());
	}

	@Test
	public void testGetOrderByClientInBuys() {
		start = LocalDateTime.now();
		Customer client = customerService.getCustomerById("" + customerId);
		Dataset<Order> retrievedOrders;
		loggerPerf.info("Start getEListByRole() [getOrderList(Order.buys.order, client)] at [{}]",
				start.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
		retrievedOrders = orderService.getOrderList(Order.buys.order, client); // Role method
		assertEquals(2, retrievedOrders.count());
		int o1Id = (customerId * 2) - 1;
		Order o1 = retrievedOrders.collectAsList().stream().filter(o -> o.getId().equals("" + o1Id)).findFirst()
				.orElse(null);
		Order expectedO1 = orders.get(o1Id + "");

		int o2Id = (customerId * 2);
		Order o2 = retrievedOrders.collectAsList().stream().filter(o -> o.getId().equals("" + o2Id)).findFirst()
				.orElse(null);
		Order expectedO2 = orders.get(o2Id + "");
		assertEquals(o1.getId(), expectedO1.getId());
		assertEquals(o1.getTotalprice(), expectedO1.getTotalprice());
		assertEquals(o1.getOrderdate(), expectedO1.getOrderdate());
		assertEquals(o2.getId(), expectedO2.getId());
		assertEquals(o2.getTotalprice(), expectedO2.getTotalprice());
		assertEquals(o2.getOrderdate(), expectedO2.getOrderdate());
		end = LocalDateTime.now();
		loggerPerf.info("End getEListByRole() at [{}]. Duration [{}]m [{}]s [{}]ms.", end,
				Duration.between(start, end).toMinutesPart(), Duration.between(start, end).toSecondsPart(),
				Duration.between(start, end).toMillisPart());
	}

	@Test
	public void testGetBuysByOrderAndCustomer() {
		start = LocalDateTime.now();
		loggerPerf.info("Start getR() [getBuys()] at [{}]", start.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
		int custId = ((Double) Math.ceil((double) orderId / 2)).intValue();
		buysDataset = buysService.getBuysList(Condition.simple(OrderAttribute.id, Operator.EQUALS, "" + orderId),
				Condition.simple(CustomerAttribute.id, Operator.EQUALS, "" + custId));
		assertEquals(1, buysDataset.count());
		end = LocalDateTime.now();
		loggerPerf.info("End getR() [getBuys()] at [{}]. Duration [{}]m [{}]s [{}]ms.", end,
				Duration.between(start, end).toMinutesPart(), Duration.between(start, end).toSecondsPart(),
				Duration.between(start, end).toMillisPart());
	}

	@Test
	public void testGetBuys() {
		start = LocalDateTime.now();
		loggerPerf.info("Start getR() [getBuys()] at [{}]", start.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
		buysDataset = buysService.getBuysList(null, null);
		assertEquals(NBINSTANCEORDERS, buysDataset.count());
		end = LocalDateTime.now();
		loggerPerf.info("End getR() [getBuys()] at [{}]. Duration [{}]m [{}]s [{}]ms.", end,
				Duration.between(start, end).toMinutesPart(), Duration.between(start, end).toSecondsPart(),
				Duration.between(start, end).toMillisPart());
	}

}
