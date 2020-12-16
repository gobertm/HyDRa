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
import org.neo4j.cypher.internal.frontend.phases.Do;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.print.Doc;
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
    private final int port;
    private final int numberofdata;

    public MongoDataInit(String databasename, String host, int port, int numberofdata) {
        this.databasename = databasename;
        this.host = host;
        this.port = port;
        this.numberofdata = numberofdata;
    }

    public static void main(String args[]){
        String mongohost = "localhost";
        String mongodbname = "mymongo";
        int mongoport = 27000;
        int nbdataobj = 100;
        MongoDataInit mongoDataInit = new MongoDataInit(mongodbname, mongohost, mongoport, nbdataobj);
//        mongoDataInit.persistData();
//        mongoDataInit.persistDataTest();
        mongoDataInit.persistDataSimpleHybrid(1);

        //Second mongo db init data
        mongohost = "localhost";
        mongodbname = "mymongo2";
        mongoport = 27100;
        nbdataobj = 100;
        mongoDataInit = new MongoDataInit(mongodbname, mongohost, mongoport, nbdataobj);
        mongoDataInit.persistDataSimpleHybrid(2);


    }

    public void persistDataSimpleHybrid(int mongoinstance) {
        Random r = new Random();
        if (mongoClient == null) {
            initConnection();
        }
        if (mongoinstance == 1) {
            MongoCollection<Document> productCollection = mongoDatabase.getCollection("productCollection");
            List<Document> documentsProductReviews = new ArrayList<Document>();
            for (int i = 0; i < numberofdata; i++) {
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

                Document product = new Document("product_ref", "product" + i)
                        .append("productDescription", RandomStringUtils.randomAlphabetic(10))
                        .append("price", RandomUtils.nextFloat())
                        .append("name", "productName" + i)
                        .append("reviews", listReviews);

                documentsProductReviews.add(product);
            }
            productCollection.insertMany(documentsProductReviews);
            logger.info("Generated and persisted [{}] documents in MongoDB [{},{}]",numberofdata,databasename,host);
        }
        if (mongoinstance == 2) {
            MongoCollection<Document> categorycollection = mongoDatabase.getCollection("categoryCollection");
            List<Document> categoryDocsList = new ArrayList<>();
            for (int i = 0; i < numberofdata; i++) {
                List<Document> productsList = new ArrayList<>();
                for (int p = 0; p<r.ints(1, 5).findFirst().getAsInt(); p++) {
                    Document product = new Document()
                            .append("id", "product" + r.ints(0, 100).findFirst().getAsInt());
                    productsList.add(product);
                }
                Document category = new Document()
                        .append("categoryname",RandomStringUtils.random(2,65,70,true,false))
                        .append("products",productsList);
                categoryDocsList.add(category);
            }
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
}
