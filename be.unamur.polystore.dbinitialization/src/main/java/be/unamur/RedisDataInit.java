package be.unamur;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

public class RedisDataInit implements DataInit{


    private final String host;
    private final int port;
    private Jedis jedis;
    static final Logger logger = LoggerFactory.getLogger(RedisDataInit.class);
    private static final int SIMPLEKEYVALUE=1;

    public static void main(String args[]) {
        RedisDataInit redisDataInit = new RedisDataInit("localhost", 6363);
        redisDataInit.initConnection();
        redisDataInit.persistData(SIMPLEKEYVALUE,10);
        redisDataInit.persistHashes(20);
    }

    private void persistHashes(int number) {
        int added=0;
        String key;
        Map<String, String> hash = new HashMap<>();
        for (int i = 0; i < number; i++) {
            key = "CLIENT:"+i;
            hash.put("name", RandomStringUtils.randomAlphabetic(3)+"_"+RandomStringUtils.randomAlphabetic(5));
            hash.put("streetnumber", String.valueOf(RandomUtils.nextInt(0, 100)));
            hash.put("street", RandomStringUtils.randomAlphabetic(9));
            jedis.hset(key, hash);
            added++;
        }
        logger.info("Added {} hashes in Redis DB",added);
    }

    public RedisDataInit(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void persistData(int model, int numberofrecords) {
        String key;
        String value;
        String productid="product";
        int added=0;
        if (jedis == null) {
            initConnection();
        }
        if(model == SIMPLEKEYVALUE){
            for (int i = 0; i < numberofrecords; i++) {
                key = "PRODUCT:"+productid+i+":PHOTO";
                value = RandomStringUtils.randomAlphabetic(8);
                jedis.set(key, value);
                added++;
            }
            logger.info("Generated and inserted [{}] key/value pairs in [{}]", added, host);

        }
        if(model == 0){
            logger.error("Please provide the int value of a pml model in order to persist compatible data");
        }

    }

    public void initConnection() {
        logger.info("Initializing connection to Jedis [{},{}]", host, port);
        jedis = new Jedis(host, port);
    }

    public void deleteAll(String dbname){
        if(jedis==null)
            initConnection();
        logger.info("Flushing all data in redis [{},{}]", host, port);
        jedis.flushAll();
    }

}
