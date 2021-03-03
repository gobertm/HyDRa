import be.unamur.MongoDataInit;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

public class VariousTests {

    MongoDataInit mongo;

    @Before
    public void setUp() {
        mongo = new MongoDataInit("mymongo", "localhost", 27100, 20);
    }

    @Test
    public void testUpdateMoviesInDirectors(){
        String[] movie = {"tt0050986","movie","SUPER DUPER MOVIE"};
        mongo.updateDirectorMovieInfo(Collections.singletonList(movie),"dummy");
    }
}
