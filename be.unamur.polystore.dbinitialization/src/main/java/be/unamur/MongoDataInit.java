package be.unamur;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class MongoDataInit implements DataInit{
    static final Logger logger = LoggerFactory.getLogger(MongoDataInit.class);
    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
    private final String databasename;
    private final String host;
    private SQLDataInit sqlDB;
    private final int port;
    private final int numberofdata;

    public MongoDataInit(String databasename, String host, int port, int numberofdata) {
        this.databasename = databasename;
        this.host = host;
        this.port = port;
        this.numberofdata = numberofdata;
    }

    public static void main(String args[]) throws SQLException {
        String mongohost = "localhost";
        String mongodbname = "mymongo";
        int mongoport = 27000;
        int nbdataobj = 30;
        MongoDataInit mongoDataInit = new MongoDataInit(mongodbname, mongohost, mongoport, nbdataobj);
        SQLDataInit sqlinit = new SQLDataInit("localhost","3307","mydb","root","password");
        sqlinit.createConnection();
        sqlinit.initStructure();
        sqlinit.initData(nbdataobj);
        mongoDataInit.setSqlDB(sqlinit);

//        mongoDataInit.persistData();
//        mongoDataInit.persistDataTest();
        mongoDataInit.persistDataSimpleHybridPmlModel(1,true);

        //Second mongo db init data
        mongohost = "localhost";
        mongodbname = "mymongo2";
        mongoport = 27100;
        mongoDataInit = new MongoDataInit(mongodbname, mongohost, mongoport, nbdataobj);
        mongoDataInit.setSqlDB(sqlinit);
        mongoDataInit.persistDataSimpleHybridPmlModel(2,true);


    }

    public void persistDataSimpleHybridPmlModel(int mongoinstance, boolean sqlUpdate) {
        String productref=null;
        int price;
        String productDesc;
        Random r = new Random();
        if (mongoClient == null) {
            initConnection();
        }
        if (mongoinstance == 1) {
            MongoCollection<Document> productCollection = mongoDatabase.getCollection("productCollection");
            List<Document> documentsProductReviews = new ArrayList<Document>();
            for (int i = 0; i < numberofdata; i++) {
                price = r.ints(0, 4).findFirst().getAsInt();
                productref = "product" + i;
                productDesc = RandomStringUtils.randomAlphabetic(10);
                Document product = new Document()
                        .append("product_ref", productref)
                        .append("productDescription", productDesc)
                        .append("price", price)
                        .append("name", "productName" + i);

                List<Document> listReviews = new ArrayList<Document>();
                for (int j = 0; j < r.ints(0, 5).findFirst().getAsInt(); j++) {
                    int usernumber = r.ints(0,100).findFirst().getAsInt();
                    int rating = r.ints(0, 5).findFirst().getAsInt();
                    Document review = new Document()
                            .append("numberofstars", rating)
                            .append("ratingstring", rating + "*")
                            .append("content", RandomStringUtils.randomAlphabetic(60));
                    listReviews.add(review);
                }
                product.append("reviews", listReviews);

                documentsProductReviews.add(product);
                if(sqlUpdate){
                    logger.info("Update product record [{}] in SQL database ", productref);
                    sqlDB.update("update ProductCatalogTable SET description='"+productDesc+"', europrice='"+price+"€' where product_id ='"+productref+"'");
                }
            }
            productCollection.insertMany(documentsProductReviews);
            logger.info("Generated and persisted [{}] documents in MongoDB [{},{}]",numberofdata,databasename,host);
        }
        if (mongoinstance == 2) {
            MongoCollection<Document> categorycollection = mongoDatabase.getCollection("categoryCollection");
            List<Document> categoryDocsList = new ArrayList<>();
            List<Document> productCatA = new ArrayList<>();
            List<Document> productCatB = new ArrayList<>();
            List<Document> productCatC = new ArrayList<>();
            String categoryName=null;
            for (int i = 0; i < numberofdata; i++) {
                productref = "product"+i;
                Document product = new Document()
                        .append("id", productref);
                switch (i % 3) {
                    case 0 :
                        categoryName="A";
                        productCatA.add(product);
                        break;
                    case 1 :
                        categoryName="B";
                        productCatB.add(product);
                        break;
                    case 2 :
                        categoryName="C";
                        productCatC.add(product);
                        break;
                }

                if(sqlUpdate) {
                    logger.info("Update product record [{}] in SQL database ", productref);
                    sqlDB.update("update ProductCatalogTable SET categoryname ='"+categoryName+"' where product_id ='"+productref+"' ");
                }
            }
            Document categoryA = new Document()
                    .append("categoryname", "A")
                    .append("products", productCatA);
            categoryDocsList.add(categoryA);
            Document categoryB = new Document()
                    .append("categoryname", "B")
                    .append("products", productCatB);
            categoryDocsList.add(categoryB);
            Document categoryC = new Document()
                    .append("categoryname", "C")
                    .append("products", productCatC);
            categoryDocsList.add(categoryC);

            categorycollection.insertMany(categoryDocsList);
            logger.info("Generated and persisted [{}] documents in MongoDB [{},{}]",numberofdata,databasename,host);
        }
    }



    public void persistData() {
        Random r = new Random();
        if (mongoClient == null) {
            initConnection();
        }

        MongoCollection<Document> collection = mongoDatabase.getCollection("product_reviews");
        List<Document> documentsProductReviews = new ArrayList<Document>();
        for (int i = 0; i < numberofdata; i++) {
            List<Document> listReviews = new ArrayList<Document>();
            for (int j = 0; j < r.ints(0, 5).findFirst().getAsInt(); j++) {
                List<Document> listComment = new ArrayList<>();
                for (int k = 0; k < r.ints(0, 10).findFirst().getAsInt(); k++) {
                    Document comment = new Document("comment", RandomStringUtils.randomAlphabetic(10))
                            .append("number",k);
                    listComment.add(comment);
                }
                int usernumber = r.ints(0,100).findFirst().getAsInt();
                int rating = r.ints(0, 5).findFirst().getAsInt();
                Document review = new Document()
                        .append("userid", "user" + usernumber)
//                        .append("user_name", "UserName" + usernumber)
                        .append("numberofstars", rating)
                        .append("ratingstring", rating+"*")
                        .append("title", "Review Title " + RandomStringUtils.randomAlphabetic(10))
                        .append("content", RandomStringUtils.randomAlphabetic(60))
                        .append("comments",listComment);
                listReviews.add(review);
            }
            Document category = new Document("category_name",RandomStringUtils.randomAlphabetic(6))
                    .append("category_description", RandomStringUtils.randomAlphabetic(15));

            Document doc = new Document("product_ref", "product" + i)
                    .append("productDescription", "This product "+RandomStringUtils.randomAlphabetic(10))
                    .append("price", RandomUtils.nextInt()+"$")
                    .append("name", "productName" + i)
                    .append("category", category)
                    .append("reviews", listReviews);

            documentsProductReviews.add(doc);
        }
        collection.insertMany(documentsProductReviews);
        logger.info("Generated and persisted [{}] documents in MongoDB [{},{}]",numberofdata,databasename,host);

    }

    public void persistDataTest() {
        Random r = new Random();
        if (mongoClient == null) {
            initConnection();
        }

        MongoCollection<Document> collection = mongoDatabase.getCollection("product_reviews");
        List<Document> documentsProductReviews = new ArrayList<Document>();
        for (int i = 0; i < numberofdata; i++) {
            int price = RandomUtils.nextInt();
            List<Document> listReviews = new ArrayList<Document>();
            for (int j = 0; j < r.ints(0, 5).findFirst().getAsInt(); j++) {
                List<Document> listComment = new ArrayList<>();
                for (int k = 0; k < r.ints(0, 10).findFirst().getAsInt(); k++) {
                    Document comment = new Document("comment", RandomStringUtils.randomAlphabetic(10))
                            .append("number",k);
                    listComment.add(comment);
                }
                int usernumber = r.ints(0,100).findFirst().getAsInt();
                int rating = r.ints(0, 5).findFirst().getAsInt();
                Document productAtt = new Document()
                        .append("name", "productName" + i)
                        .append("price", price + "$");
                List<Document> fakeLevel = new ArrayList<>();
                Document fakeAtt = new Document()
                        .append("fakeatt","XXX")
                        .append("product_attributes", productAtt);
                Document fakeAtt2 = new Document()
                        .append("fakeatt", "XXX");
                fakeLevel.add(fakeAtt);
                fakeLevel.add(fakeAtt2);


                Document review = new Document()
                        .append("fake_nested", fakeLevel)
                        .append("userid", "user" + usernumber)
//                        .append("user_name", "UserName" + usernumber)
                        .append("numberofstars", rating)
                        .append("ratingstring", rating+"*")
                        .append("title", "Review Title " + RandomStringUtils.randomAlphabetic(10))
                        .append("content", RandomStringUtils.randomAlphabetic(60))
                        .append("comments",listComment);
                listReviews.add(review);
            }
            Document category = new Document("category_name",RandomStringUtils.randomAlphabetic(6))
                    .append("category_description", RandomStringUtils.randomAlphabetic(15));

            Document doc = new Document("product_ref", "product" + i)
                    .append("productDescription", "This product "+RandomStringUtils.randomAlphabetic(10))
                    .append("category", category)
                    .append("reviews", listReviews);

            documentsProductReviews.add(doc);
        }
        collection.insertMany(documentsProductReviews);
        logger.info("Generated and persisted [{}] documents in MongoDB [{},{}]",numberofdata,databasename,host);

    }

    public void initConnection() {
        logger.info("Initialising connection to MongoClient [{}{}] and to database [{}]", host, port, databasename);
        mongoClient = MongoClients.create(
                MongoClientSettings.builder()
                        .applyToClusterSettings(builder ->
                                builder.hosts(Arrays.asList(new ServerAddress(host, port))))
                        .build());

        mongoDatabase = mongoClient.getDatabase(databasename);
    }

    public SQLDataInit getSqlDB() {
        return sqlDB;
    }

    public void setSqlDB(SQLDataInit sqlDB) {
        this.sqlDB = sqlDB;
    }
}
