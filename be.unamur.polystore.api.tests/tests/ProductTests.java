//package generated;
//
//import conditions.Operator;
//import conditions.ProductAttribute;
//import conditions.SimpleCondition;
//import dao.impl.ProductServiceImpl;
//import dao.services.ProductService;
//import org.apache.spark.sql.Dataset;
//import org.junit.BeforeClass;
//import org.junit.Test;
//import pojo.Product;
//
//public class ProductTests {
//    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProductTests.class);
//    private static ProductService productService;
//    private static SimpleCondition<ProductAttribute> productCondition;
//    private Dataset<Product> res;
//
//    @BeforeClass
//    public static void setUp() {
//        productService = new ProductServiceImpl();
////		productCondition = new SimpleCondition<>(ProductAttribute.price, Operator.EQUALS, 0);
//        productCondition = new SimpleCondition<>(ProductAttribute.id, Operator.EQUALS, "product6");
//    }
//
//    @Test
//    public void getProducts(){
//        res = productService.getProductList(null);
//        res.show(false);
//    }
//}
