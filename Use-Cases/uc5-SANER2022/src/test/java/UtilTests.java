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
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;
import pojo.Customer;
import pojo.Feedback;
import pojo.Order;
import pojo.Product;
import util.Dataset;
import util.Util;

import java.util.Arrays;
import java.util.List;

public class UtilTests {
    CustomerService customerService = new CustomerServiceImpl();
    Dataset<Customer> customerDataset;
    ProductService productService = new ProductServiceImpl();
    Dataset<Product> productDataset;
    OrderService orderService = new OrderServiceImpl();
    Dataset<Order> orderDataset;
    FeedbackServiceImpl feedbackService = new FeedbackServiceImpl();
    Dataset<Feedback> feedbackDataset;

    @Test
    public void testCreateCondition() {
        Condition orProductId = Condition.createOrCondition(ProductAttribute.id, Operator.EQUALS, new String[]{"B00004T1JH", "B00000IUX5"});
        productDataset = productService.getProductList(orProductId);
        productDataset.show();

    }
}
