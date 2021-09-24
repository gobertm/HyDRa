//package generated;
//
//import conditions.Operator;
//import conditions.ProductAttribute;
//import conditions.ReviewAttribute;
//import conditions.SimpleCondition;
//import dao.impl.ProductServiceImpl;
//import dao.impl.ReviewServiceImpl;
//import dao.services.ProductReviewService;
//import dao.services.ProductService;
//import dao.services.ReviewService;
//import org.apache.spark.sql.Dataset;
//import org.junit.BeforeClass;
//import org.junit.Test;
//import pojo.Product;
//
//public class EntityTests {
//    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(EntityTests.class);
//    private static ProductService productService;
//    private static ProductReviewService productReviewsService;
//    private static ReviewService reviewService;
//    private static SimpleCondition<ProductAttribute> productCondition;
//    private static SimpleCondition<ReviewAttribute> reviewCondition;
//
//    @BeforeClass
//    public static void setUp() {
//        productService = new ProductServiceImpl();
////        productReviewsService = new ProductReviewServiceImpl();
//        reviewService = new ReviewServiceImpl();
////		productCondition = new SimpleCondition<>(ProductAttribute.price, Operator.EQUALS, 0);
//        productCondition = new SimpleCondition<>(ProductAttribute.id, Operator.EQUALS, "product6");
//        reviewCondition = new SimpleCondition<>(ReviewAttribute.rating, Operator.EQUALS, 4);
//    }
//
//
//    // On 'fullmodel.pml'
//    @Test
//    public void testSelectProduct(){
//        /*
//       	cs.Product(id,description,price,name) -> myDocSchema.productCollection(product_ref,productDescription,price,name),
//    	cs.Product(id) -> docSchema2.categoryCollection.products(id),
//	    cs.Product(cat_name) -> docSchema2.categoryCollection(categoryname),
//	    cs.Product(id,price,description) -> myRelSchema.ProductCatalogTable(product_id,price,description),
//         */
//        // Single product. Price & description conflicted value. Cat_name only in mongo2. cat_description null
//        productCondition = new SimpleCondition<>(ProductAttribute.id, Operator.EQUALS, "product6");
//        Dataset<Product> productDataset = productService.getProductList(productCondition);
//        productDataset.show();
//
//        //TODO  : comment déterminer quel attribut prendre pour des attributs en conflits?
//    }
//
////    @Test
////    public void testSelectAllProduct(){
////        //Note that there are Products exclusive for each database. Special interest for 'productONLYMONGO2' which is in an embedded array in docDB.
////        Dataset<Product> productDataset = productService.getProductList(null);
////        productDataset.show(150);
////    }
////
////    @Test
////    public void testSelectOnAttributeOnlyInOneDB(){
////        // Category is only in Mongo, but price is in RelDB and in Mongo1 (data consistency issue spotted).
////        // We will join them in order to have a complete conceptual object.
////        productCondition = new SimpleCondition<>(ProductAttribute.cat_name, Operator.EQUALS, "A");
////        Dataset<Product> productDataset = productService.getProductList(productCondition);
////        productDataset.show();
////    }
//
////    @Test
////    public void testEmbeddedReviewsInMongo1(){
//////        Dataset<Review> reviews = reviewService.getReviewList(null);
//////        reviews.show(1000);
//////        //TODO Gestion des identifiants éventuels pour les embedded
////
////        // Uniquement les Review de Mongo1
////        //TODO cacher le MutableBoolean
////        Dataset<Review> reviews = reviewService.getReviewListInProductCollectionFromMymongo(null, new MutableBoolean());
////        reviews.show(1000);
////        System.out.println(reviews.count());
////    }
//
//}
