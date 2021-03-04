import be.unamur.MongoDataInit;
import be.unamur.RedisDataInit;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

public class VariousTests {

    MongoDataInit mongo;
    RedisDataInit redis;

    @Before
    public void setUp() {
        mongo = new MongoDataInit("mymongo", "localhost", 27100, 20);
        redis = new RedisDataInit("localhost", 6379);

    }

    @Test
    public void testUpdateMoviesInDirectors(){
        String[] movie = {"tt0050986","movie","MOVIE"};
        mongo.updateMovieInfo(Collections.singletonList(movie),"dummy");
    }

    @Test
    public void testGetTitleMovieRedis() {
        System.out.println(redis.getTitleMovie("tt0050986"));
    }
}
