import conditions.Condition;
import conditions.CustomerAttribute;
import conditions.Operator;
import conditions.ProductAttribute;
import dao.impl.CustomerServiceImpl;
import dao.impl.FeedbackServiceImpl;
import dao.impl.OrderServiceImpl;
import dao.impl.ProductServiceImpl;
import dao.services.CustomerService;
import dao.services.OrderService;
import dao.services.ProductService;
import org.junit.Test;
import pojo.Customer;
import pojo.Feedback;
import pojo.Order;
import pojo.Product;
import program.Main;
import util.Dataset;

import java.time.LocalDate;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class Tests {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Tests.class);
    CustomerService customerService = new CustomerServiceImpl();
    Dataset<Customer> customerDataset;
    ProductService productService = new ProductServiceImpl();
    Dataset<Product> productDataset;
    OrderService orderService = new OrderServiceImpl();
    Dataset<Order> orderDataset;
    FeedbackServiceImpl feedbackService = new FeedbackServiceImpl();
    Dataset<Feedback> feedbackDataset;

    @Test
    public void tesGetProducts() {
        productDataset = productService.getProductList();
        productDataset.show();
        logger.info("Retrieved [{}] products and printed first 20", productDataset.count());
        assertEquals(10116, productDataset.count());
    }

    @Test
    public void testGetOrdersOfCustomer() {
        List<Order> clientOrders;
        String customerId = "13194139544267";
        // Get Orders of a particular customer
        Condition condCustomer = Condition.simple(CustomerAttribute.id, Operator.EQUALS, customerId);
        orderDataset = orderService.getOrderList(Order.buys.order, condCustomer);
        clientOrders = orderDataset.collectAsList();
        logger.info("Client [{}] has made [{}] orders", customerId, orderDataset.count());
        clientOrders.forEach(o -> System.out.println("["+o.getOrderdate()+" - "+o.getTotalprice()+"]"));
        assertEquals(18, orderDataset.count());
    }
    
    @Test
    public void testGetCustomerFeedbacks() {
    	String prodId = "B003MA1SSI";
    	Condition condProduct = Condition.simple(ProductAttribute.id, Operator.EQUALS, prodId);
    	
    	feedbackDataset = feedbackService.getFeedbackList(condProduct, null, null); 
    	assertEquals(352, feedbackDataset.count());
    }

    @Test
    public void testInsertOrder() {
        Order newOrder = new Order("FAKEORDERID", LocalDate.now(), 999.99);
        List<String> productIdList = List.of("B0001XH6G2","B0081MPIBA");
        String customerId = "13194139544267";

//        // Add a new Order
        Condition condCustomer = Condition.simple(CustomerAttribute.id, Operator.EQUALS, customerId);
        Condition condProducts = Condition.createOrCondition(ProductAttribute.id, Operator.EQUALS, productIdList.toArray());
        productDataset = productService.getProductList(condProducts);
        orderService.insertOrder(newOrder, customerService.getCustomerById(customerId), productDataset.collectAsList());
        assertNotNull(orderService.getOrderById("FAKEORDERID"));
    }

    @Test
    public void testInconsistency() {
        Order o = orderService.getOrderById("FAKEORDERID");
        System.out.println(o);
    }

}
