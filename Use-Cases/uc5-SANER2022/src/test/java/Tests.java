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
    public void testGetProductsAbovePrice() {
        double price = 200.0;
        Condition prodPrice = Condition.simple(ProductAttribute.price, Operator.GT, price);
        productDataset = productService.getProductList(prodPrice);
        productDataset.show();
        logger.info("Retrieved [{}] products with price greater than [{}] printed first 20", productDataset.count(), price);
        assertEquals(310, productDataset.count());
    }

    @Test
    public void testGetOrdersOfCustomerByIp() {
        List<Order> clientOrders;
        String customerIPAddr = "41.138.53.138";
        // Get Orders of a particular customer
        Condition condCustomer = Condition.simple(CustomerAttribute.locationip, Operator.EQUALS, customerIPAddr);
        orderDataset = orderService.getOrderList(Order.buys.order, condCustomer);
        clientOrders = orderDataset.collectAsList();
        clientOrders.forEach(o -> System.out.println("["+o.getOrderdate()+" - "+o.getTotalprice()+"]"));
        logger.info("IP address [{}] has made [{}] orders", customerIPAddr, orderDataset.count());
        assertEquals(17, orderDataset.count());
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
        Condition condProducts = Condition.createOrCondition(ProductAttribute.id, Operator.EQUALS, productIdList.toArray());
        productDataset = productService.getProductList(condProducts);
        orderService.insertOrder(newOrder, customerService.getCustomerById(customerId), productDataset.collectAsList());
        assertNotNull(orderService.getOrderById("FAKEORDERID"));
    }

 

}
