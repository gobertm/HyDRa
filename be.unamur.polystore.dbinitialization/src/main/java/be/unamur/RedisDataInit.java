package be.unamur;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisDataInit {


    private final String host;
    private final int port;
    private Jedis jedis;
    static final Logger logger = LoggerFactory.getLogger(RedisDataInit.class);
    private static final int SIMPLEKEYVALUE=1;


    public RedisDataInit(String host, int port) {
        this.host = host;
        this.port = port;
    }
    public static void main(String args[]) {
        RedisDataInit redisDataInit = new RedisDataInit("localhost", 6363);
        redisDataInit.initConnection();
        redisDataInit.persistData(PmlModelEnum.SIMPLEKEYVALUE,10);
        redisDataInit.persistClientHashes(20);
    }

    private void persistClientHashes(int number) {
        int added=0;
        String key;
        Map<String, String> hash = new HashMap<>();
        for (int i = 0; i < number; i++) {
            key = "CLIENT:client"+i;
            hash.put("name", RandomStringUtils.randomAlphabetic(3)+"_"+RandomStringUtils.randomAlphabetic(5));
            hash.put("streetnumber", String.valueOf(RandomUtils.nextInt(0, 100)));
            hash.put("street", RandomStringUtils.randomAlphabetic(9));
            jedis.hset(key, hash);
            added++;
        }
        logger.info("Added {} CLIENT:[clientID] hashes in Redis DB",added);
    }

    // Used in IMDB data init use case.
    public String getTitleMovie(String movieId) {
        if (jedis == null) {
            initConnection();
        }
        return jedis.hget("movie:"+movieId, "title");
    }

    public void persistData(PmlModelEnum model, int numberofrecords) {
        String key;
        String value;
        String productid="product";
        int added=0;
        if (jedis == null) {
            initConnection();
        }
        if(model == PmlModelEnum.SIMPLEKEYVALUE || model == PmlModelEnum.ALLDBS){
            for (int i = 0; i < numberofrecords; i++) {
                key = "PRODUCT:"+productid+i+":PHOTO";
                value = RandomStringUtils.randomAlphabetic(8);
                jedis.set(key, value);
                added++;
            }
            logger.info("Generated and inserted [{}] key/value pairs in [{}]", added, host);

        }

        if (model == PmlModelEnum.DATAINCONS) {
            for (int i = 0; i < numberofrecords; i++) {
                key = "PRODUCT:"+productid+i+":PRICE";
                value = "00000";
                jedis.set(key, value);
                added++;
            }
            logger.info("Generated and inserted [{}] key/value pairs in [{}]", added, host);

        }

        if (model == PmlModelEnum.KVEMBEDDED) {
            String keyproduct;
            Map<String, String> hash = new HashMap<>();
            for (int i = 0; i < numberofrecords; i++) {
                keyproduct = "PRODUCT:"+productid+i;
                for (int j = 0; j < RandomUtils.nextInt(1, 4); j++) {
                    hash.clear();
                    key=keyproduct+":REVIEW:review"+i+"-"+j;
                    hash.put("content", RandomStringUtils.randomAlphabetic(20));
                    hash.put("stars", RandomUtils.nextInt(0,5)+"*");
                    jedis.hset(key, hash);
                    added++;
                }
            }
            logger.info("Generated and inserted [{}] key/value pairs in [{}]", added, host);

        }

        if (model == PmlModelEnum.KVMANYTOONE) {
            String keyproduct;
            Map<String, String> hash = new HashMap<>();
            for (int i = 0; i < numberofrecords; i++) {
                keyproduct = "PRODUCT:"+productid+i;
                for (int j = 0; j < RandomUtils.nextInt(1, 4); j++) {
                    hash.clear();
                    key=keyproduct+":REVIEW:review"+i+"-"+j;
                    hash.put("content", RandomStringUtils.randomAlphabetic(20));
                    hash.put("stars", RandomUtils.nextInt(0,5)+"*");
                    hash.put("posted_by","client"+RandomUtils.nextInt(0,20));
                    jedis.hset(key, hash);
                    added++;
                }
            }
            logger.info("Generated and inserted [{}] PRODUCT:[prodid]:REVIEW:[reviewid] key/value pairs in [{}]", added, host);
            persistClientHashes(20);
        }
        if(model == null){
            logger.error("Please provide the int value of a pml model in order to persist compatible data");
        }

    }

    public void initConnection() {
        logger.info("Initializing connection to Jedis [{},{}]", host, port);
        jedis = new Jedis(host, port);
    }

    public void deleteAll(){
        if(jedis==null)
            initConnection();
        logger.info("Flushing all data in redis [{},{}]", host, port);
        jedis.flushAll();
    }

    public void addMovie(String[] movieLine) {
        if (jedis == null) {
            initConnection();
        }
        String key;
        Map<String, String> hash = new HashMap<>();
        key = "movie:"+movieLine[0];
        hash.put("title", movieLine[2]);
        hash.put("originalTitle",movieLine[3]);
        hash.put("isAdult",movieLine[4]);
        hash.put("startYear",movieLine[5]);
        hash.put("runtimeMinutes",movieLine[7]);
        jedis.hset(key, hash);
        logger.debug("Inserted movie hset {}", key);
    }

    public void addMovie(List<String[]> movies) {
        if (jedis == null) {
            initConnection();
        }
        Pipeline pipeline = jedis.pipelined();
        String key;
        int count=0;
        Map<String, String> hash = new HashMap<>();
        logger.debug("Starting building hset movies for redis pipeline insert");
        for (String[] movieLine : movies) {
            key = "movie:"+movieLine[0];
            hash.put("title", movieLine[2]);
            hash.put("originalTitle",movieLine[3]);
            hash.put("isAdult",movieLine[4]);
            if (!movieLine[5].equals("\\N")) {
                hash.put("startYear",movieLine[5]);
            }
            if (!movieLine[7].equals("\\N")) {
                hash.put("runtimeMinutes", movieLine[7]);
            }
            pipeline.hset(key, hash);
            count++;
        }
        logger.debug("Sync of redis pipeline with {} hset keys", count);
        pipeline.sync();
        logger.info("Done pipeline insert in redis");
    }
}
