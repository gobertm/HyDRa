package program;

import conditions.*;
import dao.impl.CustomerServiceImpl;
import dao.impl.FeedbackServiceImpl;
import dao.impl.OrderServiceImpl;
import dao.impl.ProductServiceImpl;
import dao.services.CustomerService;
import dao.services.OrderService;
import dao.services.ProductService;
import pojo.Customer;
import pojo.Feedback;
import pojo.Order;
import pojo.Product;
import util.Dataset;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class Main {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Main.class);
    CustomerService customerService = new CustomerServiceImpl();
    Dataset<Customer> customerDataset;
    ProductService productService = new ProductServiceImpl();
    Dataset<Product> productDataset;
    OrderService orderService = new OrderServiceImpl();
    Dataset<Order> orderDataset;
    FeedbackServiceImpl feedbackService = new FeedbackServiceImpl();
    Dataset<Feedback> feedbackDataset;

    public static void main(String[] args) {
        Main clientProgram = new Main();
        List<Order> clientOrders = new ArrayList<>();
        // Retrieve all Products
        clientProgram.printProducts();
        // Get Orders of a particular customer
        clientOrders = clientProgram.getOrdersOfCustomer("13194139544267");
        clientOrders.forEach(o -> System.out.println("["+o.getOrderdate()+" - "+o.getTotalprice()));
        // Add a new Order
        Order newOrder = new Order("FAKEORDERID", LocalDate.now(), 999.99);
        List<String> productIdList = List.of("B0001XH6G2","B0081MPIBA");
        clientProgram.addNewOrder(newOrder, "13194139544267", productIdList);
    }



    private void printProducts() {
        productDataset = productService.getProductList(null);
        productDataset.show();
        logger.info("Retrieved and printed [{}] products ", productDataset.count());
    }

    private List<Order> getOrdersOfCustomer(String customerId) {
        Condition condCustomer = Condition.simple(CustomerAttribute.id, Operator.EQUALS, customerId);
        orderDataset = orderService.getOrderList(Order.buys.order, condCustomer);
        logger.info("Client [{}] has made [{}] orders", customerId, orderDataset.count());
        return orderDataset.collectAsList();
    }

    private void addNewOrder(Order newOrder, String customerId, List<String> productIdList) {
        Condition condCustomer = Condition.simple(CustomerAttribute.id, Operator.EQUALS, customerId);
        // Create Util function returnin a Or Condition based on a List of ID

        for (String prodId : productIdList) {

        }
        orderService.insertOrder(newOrder, customerService.getCustomerById(customerId), productDataset.collectAsList()); // Create method getById, whihch return only one element
    }
}
