package be.unamur;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class RedisDataInit implements DataInit{


    private final String host;
    private final int port;
    private Jedis jedis;
    private int numberofprojects;
    static final Logger logger = LoggerFactory.getLogger(RedisDataInit.class);


    public RedisDataInit(String host, int port, int numberofprojects) {
        this.host = host;
        this.port = port;
        this.numberofprojects = numberofprojects;
    }

    public void persistData() {
        if (jedis == null) {
            initConnection();
        }
        String key = "product";
        String value = "photoBlob";
        for (int i=0; i < numberofprojects; i++) {
            jedis.set(key.concat(Integer.toString(i)), value.concat(Integer.toString(i)));
        }
        logger.info("Generated and inserted [{}] key/value pairs in [{}]", numberofprojects, host);

    }

    @Override
    public void initConnection() {
        logger.info("Initializing connection to Jedis [{},{}]", host, port);
        jedis = new Jedis(host, port);
    }

}
