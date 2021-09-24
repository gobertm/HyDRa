//package generated;
//
//import dao.impl.ReviewServiceImpl;
//import dao.services.ReviewService;
//import org.apache.spark.sql.Dataset;
//
//import dao.impl.ProductReviewServiceImpl;
//import dao.impl.ProductServiceImpl;
//import dao.services.ProductReviewService;
//import dao.services.ProductService;
//
//import org.junit.BeforeClass;
//import org.junit.Test;
//
//import conditions.Operator;
//import conditions.ProductAttribute;
//import conditions.ReviewAttribute;
//import conditions.SimpleCondition;
//import pojo.Product;
//import pojo.Review;
//import tdo.ReviewTDO;
//
//public class OneToManyTest {
//	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OneToManyTest.class);
//	private static ProductService productService;
//	private static ProductReviewService productReviewsService;
//	private static ReviewService reviewService;
//	private static SimpleCondition<ProductAttribute> productCondition;
//	private static SimpleCondition<ReviewAttribute> reviewCondition;
//
//	@BeforeClass
//	public static void setUp() {
//		productService = new ProductServiceImpl();
//		productReviewsService = new ProductReviewServiceImpl();
//		reviewService = new ReviewServiceImpl();
////		productCondition = new SimpleCondition<>(ProductAttribute.price, Operator.EQUALS, 0);
//		productCondition = new SimpleCondition<>(ProductAttribute.id, Operator.EQUALS, "product6");
//	}
//
//	@Test
//	public void testProductReview() {
//		// Product by Review
//		reviewCondition = new SimpleCondition<>(ReviewAttribute.rating, Operator.EQUALS, 4);
//		Dataset<Product> res = productService.getProduct_roleListInProductReview(reviewCondition, null);
//		res.show(false);
//		// TODO Product sont remplis avec uniquement les attributs de Product que l'ont trouve dans la bd cible de la référence.
//		// Faudrait-il refaire un select sur les autre bds afin de garnir les autres attributs?
//
//		// Reviews by Product
//		// Note : On récupére les Review des deux sens de la référence. De Mongo2 et de ReviewTable
////		productCondition = new SimpleCondition<>(ProductAttribute.id, Operator.EQUALS, "product6");
////		Dataset<Review> resr = reviewService.getReview_roleListInProductReview(null, productCondition);
////		resr.show(false);
//
//	}
//
//}
