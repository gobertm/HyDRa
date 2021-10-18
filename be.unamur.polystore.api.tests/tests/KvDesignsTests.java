package generated;

import conditions.*;
import dao.impl.ClientServiceImpl;
import dao.impl.ProductServiceImpl;
import dao.impl.ReviewServiceImpl;
import dao.services.ClientService;
import dao.services.ProductService;
import dao.services.ReviewService;
import dbconnection.SparkConnectionMgr;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.BeforeClass;
import org.junit.Test;
import pojo.Client;
import pojo.Product;
import pojo.Review;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class KvDesignsTests {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KvDesignsTests.class);
    private static ProductService productService = new ProductServiceImpl();
    private static ClientService clientService = new ClientServiceImpl();
    private static SimpleCondition<ProductAttribute> productCondition;
    private static ReviewService reviewService = new ReviewServiceImpl();
    private static SimpleCondition<ReviewAttribute> reviewCondition;
    private static SimpleCondition<ClientAttribute> clientCondition;
    private final static int NBINSTANCE = 20;
    private static Jedis jedis;
    private Dataset<Client> resClient;
    private Dataset<Product> resProduct;
    private Dataset<Review> resReview;

    @BeforeClass
    public static void setUp() {
        String key;
        String value;
        String productid="product";
        int added=0;
        String keyproduct;
        Map<String, String> hash = new HashMap<>();

        jedis = new Jedis("localhost", 6379);
        logger.info("FLUSHALL REDIS");
        jedis.flushAll();

        // kvProductName & kvProdPrice
        for (int i = 0; i < NBINSTANCE; i++) {
            jedis.set("PRODUCT:product" + i + ":NAME", "productname" + i);
            jedis.set("PRODUCT:product" + i + ":PRICE", RandomUtils.nextFloat(0, 1000) + "$");
        }
        logger.info("Generated and inserted [{}] PRODUCT:[prodid]:REVIEW:[reviewid] key/value pairs in [{}]", added, "localhost");


        // KVClient data
        added=0;
        hash.clear();
        for (int i = 0; i < NBINSTANCE; i++) {
            key = "CLIENT:client"+i;
            hash.put("name", "clientfirstname"+i+"_clientlastname"+i);
            hash.put("streetnumber", String.valueOf(RandomUtils.nextInt(0, 100)));
            hash.put("street", RandomStringUtils.randomAlphabetic(9));
            jedis.hset(key, hash);
            added++;
        }
        logger.info("Added {} CLIENT:[clientID] hashes in Redis DB",added);

        // KVReview data
        added=0;
        hash.clear();
        for (int i = 0; i < NBINSTANCE; i++) {
            for (int j = 1; j <= 2; j++) {
                key = "REVIEW:review"+i+"-"+j;
                hash.put("content", RandomStringUtils.randomAlphabetic(20));
                hash.put("stars", RandomUtils.nextInt(0,5)+"*");
                hash.put("posted_by","client"+i%2);
                jedis.hset(key, hash);
                added++;
            }
        }
        logger.info("Added {} REVIEW:[reviewid] hashes in Redis DB",added);

        // kvProductList
        for (int i = 0; i < 10; i++) {
            jedis.lpush("PRODUCT:product"+i+":REVIEWS", "review"+i+"-1","review"+i+"-2");
        }
    }

    @Test
    public void testGetEntity(){
        resProduct = productService.getProductList(null);
        resProduct.show(false);
        assertEquals(NBINSTANCE,resProduct.count());
        resReview = reviewService.getReviewList(null);
        assertEquals(NBINSTANCE*2,resReview.count());
        resClient = clientService.getClientList(null);
        assertEquals(NBINSTANCE,resClient.count());

        reviewCondition = new SimpleCondition<>(ReviewAttribute.id, Operator.EQUALS, "review14-1");
        Dataset<Review> rev = reviewService.getReviewList(reviewCondition);
        assertEquals(1,rev.count());
    }

    @Test
    public void testGetAttributeAcrossKeyValuePairs(){
        productCondition = Condition.simple(ProductAttribute.id, Operator.EQUALS, "product5");
        resProduct = productService.getProductList(productCondition);
        resProduct.show();
        Product p = resProduct.collectAsList().get(0);
        assertEquals(1, resProduct.count());
        assertEquals("productname5",p.getName());
        assertNotNull(p.getPrice());
        assertNull(p.getCat_description());
    }

    @Test
    public void testGetRefKVHashToKVHash(){
        // KVReview has a ref to KVClient
        // Get client by review
        reviewCondition = new SimpleCondition<>(ReviewAttribute.id, Operator.EQUALS, "review14-1");
        resReview = reviewService.getReviewList(reviewCondition);
        Client c = clientService.getClient(Client.reviewClient.poster, resReview.collectAsList().get(0));
        assertEquals("client0",c.getId());
        // Get review by client
        resReview = reviewService.getReviewList(Review.reviewClient.review, c);
        resReview.show();
        assertEquals(20, resReview.count());
    }

    @Test
    public void testGetRefKVListToHash(){
        // kvProduct contains a list with ReviewID
        // Get review by product
        productCondition = Condition.simple(ProductAttribute.id, Operator.EQUALS, "product0");
        resReview = reviewService.getReviewList(Review.productReview.reviews, productCondition);
        resReview.show();
        assertEquals(2,resReview.count());
        // Get product by review
        reviewCondition = Condition.simple(ReviewAttribute.id, Operator.EQUALS, "review0-1");
        Review review = reviewService.getReviewList(reviewCondition).collectAsList().get(0);
        resProduct = productService.getProductList(Product.productReview.product, reviewCondition);
        resProduct.show();
        assertEquals(1, resProduct.count());
    }

    @Test
    public void getListWithJedis(){
        ScanParams scanParams = new ScanParams().match("PRODUCT:*:REVIEWS").count(100);
        String cur = ScanParams.SCAN_POINTER_START;
        boolean cycleIsFinished = false;
        Map<String, List<String>> listKV = new HashMap<>();
        while(!cycleIsFinished) {
            ScanResult<String> scanResult =
                    jedis.scan(cur, scanParams);
            List<String> keysList = scanResult.getResult();

            //do whatever with the key-value pairs in result
            for (String key : keysList) {
                List<String> listvalues = jedis.lrange(key, 0, -1);
                listKV.put(key, listvalues);
            }
            cur = scanResult.getCursor();
            if (cur.equals("0")) {
                cycleIsFinished = true;
            }
        }
        listKV.forEach((k,v) -> System.out.println(k+" - ["+String.join(",",v)+"]"));

//        // Try to convert this Map to a Dataset
        SparkConf sparkConf = new SparkConf()
                .setAppName("MyApp")
                .setMaster("local[*]")
                .set("spark.redis.host", "localhost")
                .set("spark.redis.port", "6379");
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        // Create a dataset row which flattens the list of values. So instead of having <productid, [reviewid1, reviewid2]>
        // we have (productid, reviewid1) (productid, reviewid2) and this can be converted easily into ProductTDO.
        List<Row> rows = new ArrayList();
        StructType structType = new StructType(new StructField[] {
                DataTypes.createStructField("_id", DataTypes.StringType, true), //technical field to store the key.
                DataTypes.createStructField("prodid", DataTypes.StringType, true)
                ,
                DataTypes.createStructField("idreview", DataTypes.StringType, true)
        });
        listKV.forEach((k,v) -> v.forEach(vv -> rows.add(RowFactory.create(k,k,vv))));
        Dataset<Row> data = spark.createDataFrame(rows, structType);
        data.show();
    }

}
