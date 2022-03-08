import conditions.Condition;
import conditions.Operator;
import conditions.OrderAttribute;
import conditions.ProductAttribute;
import dao.impl.Composed_ofServiceImpl;
import dao.impl.OrderServiceImpl;
import dao.impl.ProductServiceImpl;
import dao.services.*;
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

import pojo.*;
import redis.clients.jedis.Jedis;

import util.Dataset;

import java.sql.Connection;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

//Conceptual tests on CS3 model. Extends CS1 Tests as those are still valid.
public class CS3PSbKeyRelTests {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CS3PSbKeyRelTests.class);
	static final org.slf4j.Logger loggerPerf = org.slf4j.LoggerFactory.getLogger("LogPerf");

	// cannot exceed 30 because of the date and must be even
	static final int NBINSTANCEPRODUCTS = 20;
	static final int NBINSTANCEORDERS = NBINSTANCEPRODUCTS / 2;
	static int productId = 10;
	static int orderId = 5;
	static MongoClient mongoClient;
	static Connection connection;
	static Jedis jedis;

	static Map<String, Order> orders = new HashMap<String, Order>();
	static Map<String, Product> products = new HashMap<String, Product>();
	static Map<String, Composed_of> compositions = new HashMap<String, Composed_of>();

	private Dataset<Order> orderDataset;
	private OrderService orderService = new OrderServiceImpl();
	private Dataset<Product> productDataset;
	private ProductService productService = new ProductServiceImpl();
	private Dataset<Composed_of> composedOfDataset;
	private Composed_ofService composed_ofService = new Composed_ofServiceImpl();
	private LocalDateTime start;
	private LocalDateTime end;

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
			products = new HashMap<String, Product>();
			for (int i = 1; i <= NBINSTANCEPRODUCTS; i++) {
				Product p = new Product();
				p.setId("" + i);
				p.setTitle("title" + i);
				p.setPrice(Double.valueOf(i + "d"));
				p.setPhoto("photo" + i);
				products.put("" + i, p);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			orders = new HashMap<String, Order>();
			compositions = new HashMap<String, Composed_of>();
			for (int i = 1; i <= NBINSTANCEORDERS; i++) {
				Order o = new Order();
				o.setId("" + i);
				o.setTotalprice(Double.valueOf(i + "d"));
				String date = "2022-08-" + (i < 10 ? "0" + i : i);
				LocalDate localDate = LocalDate.parse(date);
				o.setOrderdate(localDate);

				int prod1 = ((i - 1) * 2) + 1;
				int prod2 = prod1 + 1;

				Product p1 = products.get("" + prod1);
				Product p2 = products.get("" + prod2);
				o._setOrderedProductsList(Arrays.asList(p1, p2));
				compositions.put("" + o.getId() + ":" + p1.getId(), new Composed_of(o, p1));
				compositions.put("" + o.getId() + ":" + p2.getId(), new Composed_of(o, p2));
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

		for (Product p : products.values())
			productService.insertProduct(p);

		for (Order o : orders.values())
			orderService.insertOrder(o, o._getOrderedProductsList());
	}

	@Test
	public void testGetRComposedOf() {
		start = LocalDateTime.now();
		loggerPerf.info("Start getR() [getComposedOf()] at [{}]", start.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
		composedOfDataset = composed_ofService.getComposed_ofList(null, null);
		assertEquals(NBINSTANCEORDERS * 2, composedOfDataset.count());
		for (Composed_of comp : composedOfDataset.collectAsList()) {
			String key = comp.getOrderP().getId() + ":" + comp.getOrderedProducts().getId();
			Composed_of comp2 = compositions.get(key);
			equals(comp.getOrderedProducts(), comp2.getOrderedProducts());
			equals(comp.getOrderP(), comp2.getOrderP());
			compositions.remove(key);
		}
		assertTrue(compositions.isEmpty());
		end = LocalDateTime.now();
		loggerPerf.info("End getR() [getComposedOf()] at [{}]. Duration [{}]h [{}]m [{}]s [{}]ms.", end,
				Duration.between(start, end).toHoursPart(), Duration.between(start, end).toMinutesPart(),
				Duration.between(start, end).toSecondsPart(), Duration.between(start, end).toMillisPart());
	}

	@Test
	public void testGetRComposedOfByProductCondition() {
		start = LocalDateTime.now();
		loggerPerf.info("Start getR() [getComposedOf()] at [{}]", start.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

		Condition<ProductAttribute> cond = Condition.simple(ProductAttribute.id, Operator.EQUALS, "" + productId);
		composedOfDataset = composed_ofService.getComposed_ofList(null, cond);
		assertEquals(1, composedOfDataset.count());
		String compKey = composedOfDataset.first().getOrderP().getId() + ":"
				+ composedOfDataset.first().getOrderedProducts().getId();
		
		String expectedKey = ((Double) Math.ceil((double) productId / 2)).intValue() + ":" + productId;
		assertEquals(compKey, expectedKey);
		Composed_of expectedComp = compositions.get(expectedKey);
		equals(composedOfDataset.first().getOrderedProducts(), expectedComp.getOrderedProducts());
		equals(composedOfDataset.first().getOrderP(), expectedComp.getOrderP());
		
		end = LocalDateTime.now();
		loggerPerf.info("End getR() [getComposedOf()] at [{}]. Duration [{}]h [{}]m [{}]s [{}]ms.", end,
				Duration.between(start, end).toHoursPart(), Duration.between(start, end).toMinutesPart(),
				Duration.between(start, end).toSecondsPart(), Duration.between(start, end).toMillisPart());
	}

	
	@Test
	public void testGetRComposedOfByOrderCondition() {
		start = LocalDateTime.now();
		loggerPerf.info("Start getR() [getComposedOf()] at [{}]", start.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

		Condition<OrderAttribute> cond = Condition.simple(OrderAttribute.id, Operator.EQUALS, "" + orderId);
		composedOfDataset = composed_ofService.getComposed_ofList(cond, null);
		assertEquals(2, composedOfDataset.count());
		String compKey1 = composedOfDataset.collectAsList().get(0).getOrderP().getId() + ":"
				+ composedOfDataset.collectAsList().get(0).getOrderedProducts().getId();
		String compKey2 = composedOfDataset.collectAsList().get(1).getOrderP().getId() + ":"
				+ composedOfDataset.collectAsList().get(1).getOrderedProducts().getId();
		
		
		String expectedKey1 = orderId + ":" + ((orderId * 2) - 1);
		String expectedKey2 = orderId + ":" + (orderId * 2);
		HashSet<String> expectedKeys = new HashSet<String>(Arrays.asList(expectedKey1, expectedKey2));
		HashSet<String> compKeys = new HashSet<String>(Arrays.asList(compKey1, compKey2));
		assertEquals(expectedKeys, compKeys);
		
		Composed_of comp1 = composedOfDataset.collectAsList().get(0);
		Composed_of comp2 = composedOfDataset.collectAsList().get(1);
		Composed_of expectedComp1 = compositions.get(compKey1);
		Composed_of expectedComp2 = compositions.get(compKey2);
		equals(comp1.getOrderedProducts(), expectedComp1.getOrderedProducts());
		equals(comp1.getOrderP(), expectedComp1.getOrderP());
		equals(comp2.getOrderedProducts(), expectedComp2.getOrderedProducts());
		equals(comp2.getOrderP(), expectedComp2.getOrderP());
		
		end = LocalDateTime.now();
		loggerPerf.info("End getR() [getComposedOf()] at [{}]. Duration [{}]h [{}]m [{}]s [{}]ms.", end,
				Duration.between(start, end).toHoursPart(), Duration.between(start, end).toMinutesPart(),
				Duration.between(start, end).toSecondsPart(), Duration.between(start, end).toMillisPart());
	}

	
	@Test
	public void testGetOrderByOneProduct() {
		start = LocalDateTime.now();
		Product product = productService.getProductById("" + productId);
		Product expectedProduct = products.get("" + productId);
		equals(product, expectedProduct);
		loggerPerf.info("Start getE1ByE2() [getOrderByProduct()] at [{}]",
				start.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
		orderDataset = orderService.getOrderList(Order.composed_of.orderP, product);
		assertEquals(1, orderDataset.count());
		Order o = orderDataset.first();
		Order expectedOrder = orders.get("" + ((Double) Math.ceil((double) productId / 2)).intValue());
		equals(o, expectedOrder);
		end = LocalDateTime.now();
		loggerPerf.info("End getR() [getOrderByProduct()] at [{}]. Duration [{}]h [{}]m [{}]s [{}]ms.", end,
				Duration.between(start, end).toHoursPart(), Duration.between(start, end).toMinutesPart(),
				Duration.between(start, end).toSecondsPart(), Duration.between(start, end).toMillisPart());
	}

	@Test
	public void testGetProductByOneOrder() {
		start = LocalDateTime.now();
		Order order = orderService.getOrderById("" + orderId);
		Order expectedOrder = orders.get("" + orderId);
		equals(order, expectedOrder);
		loggerPerf.info("Start getE2ByE1() [GetProductByOneOrder()] at [{}]",
				start.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
		productDataset = productService.getProductList(Product.composed_of.orderedProducts, order);
		assertEquals(2, productDataset.count());
		int expectedP1Id = (orderId * 2) - 1;
		int expectedP2Id = expectedP1Id + 1;
		Set<String> expectedProductIds = new HashSet<String>(Arrays.asList("" + expectedP1Id, "" + expectedP2Id));
		Product p1 = productDataset.collectAsList().get(0);
		Product p2 = productDataset.collectAsList().get(1);

		Product expectedP1 = products.get(p1.getId());
		Product expectedP2 = products.get(p2.getId());
		equals(p1, expectedP1);
		equals(p2, expectedP2);

		expectedProductIds.remove(p1.getId());
		expectedProductIds.remove(p2.getId());
		assertTrue(expectedProductIds.isEmpty());

		end = LocalDateTime.now();
		loggerPerf.info("End getR() [GetProductByOneOrder()] at [{}]. Duration [{}]h [{}]m [{}]s [{}]ms.", end,
				Duration.between(start, end).toHoursPart(), Duration.between(start, end).toMinutesPart(),
				Duration.between(start, end).toSecondsPart(), Duration.between(start, end).toMillisPart());
	}

	@Test
	public void testGetOrdersByNProductsWithCondition() {
//        select count(distinct A.orderid) from detailOrderTable A where A.productid IN (select (B.asin) from productTable B where price > 750.0);
		start = LocalDateTime.now();
		loggerPerf.info("Start getE1ByE2(E2Condition) [GetOrdersByNProductsWithCondition()] at [{}]",
				start.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
		Condition<ProductAttribute> productCondition = Condition.simple(ProductAttribute.price, Operator.GT,
				Double.valueOf(productId + "d")); // 5 products
		orderDataset = orderService.getOrderList(Order.composed_of.orderP, productCondition);
		assertEquals((NBINSTANCEPRODUCTS - productId) / 2, orderDataset.count());

		Set<String> expectedOrderIds = new HashSet<String>();
		for (int i = productId + 1; i <= NBINSTANCEPRODUCTS; i++) {
			expectedOrderIds.add("" + ((Double) Math.ceil((double) i / 2)).intValue());
		}

		for (Order o : orderDataset.collectAsList()) {
			Order expectedO = orders.get(o.getId());
			equals(o, expectedO);
			expectedOrderIds.remove(o.getId());
		}

		assertTrue(expectedOrderIds.isEmpty());

		end = LocalDateTime.now();
		loggerPerf.info(
				"End getE1ByE2(E2Condition) [GetOrdersByNProductsWithCondition()] at [{}]. Duration [{}]h [{}]m [{}]s [{}]ms.",
				end, Duration.between(start, end).toHoursPart(), Duration.between(start, end).toMinutesPart(),
				Duration.between(start, end).toSecondsPart(), Duration.between(start, end).toMillisPart());
	}

	@Test
	public void testGetProductsByNOrdersWithCondition() {
//        select count(distinct A.productid) from detailOrderTable A where A.orderid IN (select (B.orderId) from orderTable B where orderDate = '2022-09-01');
		start = LocalDateTime.now();
		loggerPerf.info("Start getE2ByE1(E1Condition) [GetOrdersByNProductsWithCondition()] at [{}]",
				start.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
		Condition<OrderAttribute> orderCondition = Condition.simple(OrderAttribute.orderdate, Operator.EQUALS,
				LocalDate.of(2022, 8, orderId)); // = 200 Orders
		productDataset = productService.getProductList(Product.composed_of.orderedProducts, orderCondition);
		assertEquals(2, productDataset.count());

		Set<String> expectedProductIds = new HashSet<String>();
		expectedProductIds.add((orderId * 2) + "");
		expectedProductIds.add(((orderId * 2) - 1) + "");
		Product p1 = productDataset.collectAsList().get(0);
		Product p2 = productDataset.collectAsList().get(1);
		Product expectedP1 = products.get(p1.getId());
		Product expectedP2 = products.get(p2.getId());
		equals(p1, expectedP1);
		equals(p2, expectedP2);
		expectedProductIds.remove(p1.getId());
		expectedProductIds.remove(p2.getId());
		assertTrue(expectedProductIds.isEmpty());

		end = LocalDateTime.now();
		loggerPerf.info(
				"End getE2ByE1(E1Condition) [GetOrdersByNProductsWithCondition()] at [{}]. Duration [{}]h [{}]m [{}]s [{}]ms.",
				end, Duration.between(start, end).toHoursPart(), Duration.between(start, end).toMinutesPart(),
				Duration.between(start, end).toSecondsPart(), Duration.between(start, end).toMillisPart());
	}

	private static void equals(Order o1, Order o2) {
		assertNotNull(o1);
		assertNotNull(o2);
		assertEquals(o1.getId(), o2.getId());
		assertNotNull(o1.getId());
		assertEquals(o1.getOrderdate(), o2.getOrderdate());
		assertNotNull(o1.getOrderdate());
		assertEquals(o1.getTotalprice(), o1.getTotalprice());
		assertNotNull(o1.getTotalprice());
	}

	private static void equals(Product p1, Product p2) {
		assertNotNull(p1);
		assertNotNull(p2);
		assertEquals(p1.getId(), p2.getId());
		assertNotNull(p1.getId());
		assertEquals(p1.getTitle(), p2.getTitle());
		assertNotNull(p1.getTitle());
		assertEquals(p1.getPhoto(), p2.getPhoto());
		assertNotNull(p1.getPhoto());
		assertEquals(p1.getPrice(), p2.getPrice());
		assertNotNull(p1.getPrice());
	}

}
