import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;

import conditions.Condition;
import conditions.Operator;
import conditions.ProductAttribute;
import dao.impl.CustomerServiceImpl;
import dao.impl.OrderServiceImpl;
import dao.services.CustomerService;
import dao.services.OrderService;
import pojo.Customer;
import pojo.Order;

import static org.apache.spark.sql.functions.*;

public class Test {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Test.class);
	private static CustomerService custService = new CustomerServiceImpl();
	private static OrderService orderService = new OrderServiceImpl();

	@org.junit.Test
	public void testGetBiggestDuffBeerConsumer() {
		// searching for the orders of Duff beers.
		Condition<ProductAttribute> duffCond = Condition.simple(ProductAttribute.label, Operator.EQUALS, "Duff Beer");
		
		// This conceptual query accesses the MongoDB (i.e., orders) AND MySQL databases
		// (i.e., products), and joins both datasets.
		Dataset<Order> orders = orderService.getOrderList(Order.of.order, duffCond);

		// one uses predefined Hadoop Spark functions (i.e., desc and limit) to return
		// the order having the biggest quantity of Duff beers
		orders = orders.sort(desc("quantity")).limit(1);
		if (!orders.isEmpty()) {
			Order biggestQuantityOrder = orders.first();
			// one queries the MongoDB db to retrieve the customer who places that order
			Customer biggestDuffConsumer = custService.getCustomer(Customer.places.buyer, biggestQuantityOrder);
			logger.info("The biggest Duff consumer is " + biggestDuffConsumer.getFirstName() + " "
					+ biggestDuffConsumer.getLastName() + ", living in " + biggestDuffConsumer.getAddress()
					+ ", who ordered " + biggestQuantityOrder.getQuantity() + " units");
		}
		
	}

}
