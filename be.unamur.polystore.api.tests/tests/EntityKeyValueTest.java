//package generated;
//
//import conditions.*;
//import dao.impl.ClientServiceImpl;
//import dao.impl.ProductServiceImpl;
//import dao.impl.ReviewServiceImpl;
//import dao.services.ClientService;
//import dao.services.ProductService;
//import dao.services.ReviewService;
//import org.apache.commons.lang.mutable.MutableBoolean;
//import org.apache.spark.sql.Dataset;
//import org.junit.BeforeClass;
//import org.junit.Test;
//import pojo.Client;
//import pojo.Product;
//import pojo.Review;
//
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
//public class EntityKeyValueTest {
//    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EntityKeyValueTest.class);
//    private static ProductService productService;
//    private static ClientService clientService;
//    private static SimpleCondition<ProductAttribute> productCondition;
//    private static ReviewService reviewService;
//    private static SimpleCondition<ReviewAttribute> reviewCondition;
//    private static SimpleCondition<ClientAttribute> clientCondition;
//
//    @BeforeClass
//    public static void setUp() {
//        productService = new ProductServiceImpl();
//        productCondition = new SimpleCondition<>(ProductAttribute.id, Operator.EQUALS, "product6");
//        clientService = new ClientServiceImpl();
//        reviewService = new ReviewServiceImpl();
////        reviewCondition = new SimpleCondition<>(ReviewAttribute.rating, Operator.EQUALS, 4);
//        reviewCondition = new SimpleCondition<>(ReviewAttribute.id, Operator.EQUALS, "review14-1");
//        clientCondition = new SimpleCondition<>(ClientAttribute.id, Operator.EQUALS, "client16");
//    }
//
//    @Test
//    public void testKeyValueStringValue(){
//        Dataset<Product> res;
////        res = productService.getProductList(null);
////        res = productService.getProductList(productCondition);
////       res.show(false);
////        System.out.println(res.count());
//
//        Dataset<Review> rev = reviewService.getReviewList(reviewCondition);
////        Dataset<Review> rev = reviewService.getReviewList(null);
//        rev.show();
//        System.out.println(rev.count());
//    }
//
//    @Test
//    public void testKeyValueHashes(){
//        Dataset<Client> res;
//        res = clientService.getClientList(null);
//        res.show(false);
//    }
//
//    @Test
//    public void testManyToOne(){
//        Dataset<Client> res;
//        Dataset<Product> resProduct;
//        Dataset<Review> resReview;
//        resReview = reviewService.getReviewListInReviewClient(clientCondition, null);
//        resReview.show();
//        res = clientService.getPosterListInReviewClient(null, reviewCondition);
//        res.show();
//    }
//
//    @Test
//    public void getByPOJO() {
//        Client client = new Client();
//        client.setId("client16");
//        reviewService.getReviewListInReviewClientByPoster(client);
//    }
//
//    @Test
//    public void dummy(){
//        String value = "CLIENT:*:*";
//        Pattern pattern = Pattern.compile("\\*");
//        Matcher match = pattern.matcher(value);
//        if(match.results().count()==1){
//            System.out.println("SPARK id pattern");
//        }
//    }
//}
