package be.unamur;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.sql.SQLException;
import java.util.*;

import static com.mongodb.client.model.Filters.elemMatch;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;

public class MongoDataInit {
    static final Logger logger = LoggerFactory.getLogger(MongoDataInit.class);
    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
    private final String databasename;
    private final String host;
    private SQLDataInit sqlDB;
    private RedisDataInit redis;
    private final int port;
    private final int numberofdata;
//    private static final int SIMPLEHYBRID=0, ONETOMANYPML=1, ONETOMANYMONGOTOREL = 2, ALLDBS= 3;


    public MongoDataInit(String databasename, String host, int port, int numberofdata) {
        this.databasename = databasename;
        this.host = host;
        this.port = port;
        this.numberofdata = numberofdata;
    }

    public static void main(String args[]) throws SQLException {
        int nbdataobj = 30;

        // Mongo DB 1
        String mongohost = "localhost";
        String mongodbname = "mymongo";
        int mongoport = 27000;

        // Mongo DB 2
        String mongohost2 = "localhost";
        String mongodbname2 = "mymongo2";
        int mongoport2 = 27100;

        // Relational DB Init
        SQLDataInit sqlinit = new SQLDataInit("localhost","3307","mydb","root","password");
        sqlinit.initConnection();
        // Structure init
//        sqlinit.initStructure(1);
        // Data init
//        sqlinit.initData(nbdataobj);

        // Model 'simplehybrid.pml'
        // Mongo DB 1
//        MongoDataInit mongoDataInit1 = new MongoDataInit(mongodbname, mongohost, mongoport, nbdataobj);
//        mongoDataInit1.setSqlDB(sqlinit);
//        mongoDataInit1.persistDataPmlModel(1,true, PmlModelEnum.SIMPLEHYBRID);
        // Mongo DB 2
//        MongoDataInit mongoDataInit2 = new MongoDataInit(mongodbname2, mongohost2, mongoport2, nbdataobj);
//        mongoDataInit2.setSqlDB(sqlinit);
//        mongoDataInit2.persistDataSimpleHybridPmlModel(2,true,PmlModelEnum.SIMPLEHYBRID);

        // Model 'onetomanyMongoToRel.pml'
        // On Mongo DB 2
        MongoDataInit mongoDataInit2 = new MongoDataInit(mongodbname2, mongohost2, mongoport2, nbdataobj);
        mongoDataInit2.setSqlDB(sqlinit);
        mongoDataInit2.persistDataPmlModel(2,true,PmlModelEnum.ONETOMANYMONGOTOREL);

        // Old models 'simple_rel_doc.pml' ,..
//        mongoDataInit.persistData();
//        mongoDataInit.persistDataTest();
    }

    public void persistDataPmlModel(int mongoinstance, boolean sqlUpdate, PmlModelEnum pmlmodel) {
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
                    sqlDB.executeSQL("update ProductCatalogTable SET description='"+productDesc+"', europrice='"+price+"€' where product_id ='"+productref+"'");
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
                        .append("id", productref)
                        .append("name", "productName" + i);
                if (pmlmodel == PmlModelEnum.ONETOMANYMONGOTOREL) {
                    List<Document> reviewsOfProduct = new ArrayList<>();
                    for (int j = 0; j < RandomUtils.nextInt(0, 5); j++) {
                        String review_id = "review"+i+"-"+j;
                        Document review = new Document()
                                .append("review_ref",review_id)
                                .append("rating",RandomUtils.nextInt(0,5));
                        reviewsOfProduct.add(review);
                        if(sqlUpdate){
                            logger.info("Insert corresponding review in ReviewTable [{}] in SQL database ", review_id);
                            sqlDB.executeSQL("insert into ReviewTable(review_id, rating, content, product_ref) VALUES ('"+review_id+"',"+RandomUtils.nextInt(0,5)+",'"+RandomStringUtils.randomAlphabetic(60)+"','"+productref + "')");

                        }
                    }
                    product.append("reviews", reviewsOfProduct);
                }
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

//                if(sqlUpdate) {
//                    logger.info("Update product record [{}] in SQL database ", productref);
//                    sqlDB.executeSQL("update ProductCatalogTable SET categoryname ='"+categoryName+"' where product_id ='"+productref+"' ");
//                }
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

    public void persistDataSimpleRel_DocPMLModel() {
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

    public void deleteAll(String collectionToDrop) {
        initConnection();
        logger.info("Dropping mongo database [{}]", databasename);
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionToDrop);
        collection.drop();
//        mongoDatabase.drop();
    }
    public String getDatabasename() {
        return databasename;
    }

    public SQLDataInit getSqlDB() {
        return sqlDB;
    }

    public void setSqlDB(SQLDataInit sqlDB) {
        this.sqlDB = sqlDB;
    }

    private Document getActorDocument(String[] actor) {
        Document actorDoc = new Document();
        String title;
        String[] movieLine;
        actorDoc.append("id",actor[0])
                .append("fullname", actor[1])
                .append("birthyear",actor[2]);
        if (!(actor[2].contains("\\N")))
            actorDoc.append("birthyear",actor[2]);
        if (!(actor[3].contains("\\N")))
            actorDoc.append("deathyear",actor[3]);
        List<Document> titlesDocs = new ArrayList<>();
        String[] titles = actor[5].split(",");
        for (String titleId : Arrays.asList(titles)) {
            title = redis.getTitleMovie(titleId);
            Document rating = new Document("rate", String.format("%.1f", RandomUtils.nextFloat(0, 10)) + "/10")
                    .append("numberofvotes", RandomUtils.nextInt(0, 100000));
            if (title != null) {
                Document movie = new Document("id", titleId)
                        .append("title",title )
                        .append("rating", rating);
                titlesDocs.add(movie);
            }
        }
        actorDoc.append("movies", titlesDocs);
        return actorDoc;
    }

    public void updateMovieInfo(List<String[]> movies, String collectionWithMovies) {
        //{movies:{$elemMatch:{id:"tt0050986"}}}
        // movies.$.title : "dd"
        if (mongoClient == null) {
            initConnection();
        }
        Bson filter=null;
        Bson titleUpdate = null, ratingUpdate=null, votesUpdate=null,updates=null;
        UpdateResult res;
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionWithMovies);
        for (String[] movie : movies) {
            filter = elemMatch("movies",Document.parse("{id:'"+movie[0]+"'}"));
            titleUpdate = set("movies.$.title", movie[2]);
            ratingUpdate = set("movies.$.rating.rate", String.format("%.1f",RandomUtils.nextFloat(0,10))+"/10");
            votesUpdate = set("movies.$.rating.numberofvotes",RandomUtils.nextInt(0,100000));
            updates = combine(titleUpdate, ratingUpdate, votesUpdate);
            res = collection.updateMany(filter,updates);
            logger.debug("Updated {} actor documents movie title in {} {} - {}",res.getModifiedCount(),collectionWithMovies, movie[0], movie[2]);
        }
    }

    public void addActors(List<String[]> actors, String actorCollection) {
        if (mongoClient == null) {
            initConnection();
        }
        int i=0;
        MongoCollection<Document> collection = mongoDatabase.getCollection(actorCollection);
        List<Document> actorDocList = new ArrayList<>();
        for (String[] actorLine : actors) {
            actorDocList.add(getActorDocument(actorLine));
            i++;
            if(i % 1000000==0){
                logger.debug("Starting bulk insert 1 000 000 documents in mongo");
                collection.insertMany(actorDocList);
                logger.debug("Inserted actors documents", i);
                actorDocList.clear();
            }
        }
        logger.debug("Final bulk insert in mongo documents",i);
        collection.insertMany(actorDocList);
        logger.debug("Inserted actors [{}] documents", i);
    }

    public void dropDatabase() {
        initConnection();
        logger.info("Dropping mongo database [{}]", databasename);
        mongoDatabase.drop();
    }

    public void setRedis(RedisDataInit redisDataInit) {
        this.redis= redisDataInit;
    }
}
