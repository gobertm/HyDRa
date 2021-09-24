package generated;
import conditions.*;
import dao.impl.ActorServiceImpl;
import dao.impl.DirectorServiceImpl;
import dao.impl.MovieServiceImpl;
import dao.services.ActorService;
import dao.services.DirectorService;
import dao.services.MovieService;
import org.apache.spark.sql.Dataset;
import org.junit.Test;
import pojo.Actor;
import pojo.Director;
import pojo.Movie;

import java.util.HashSet;
import java.util.Set;

public class IMDBTests {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IMDBTests.class);
    DirectorService directorService = new DirectorServiceImpl();
    Dataset<Director> directorDataset;
    MovieService movieService = new MovieServiceImpl();
    Dataset<Movie> movieDataset;
    ActorService actorService = new ActorServiceImpl();
    Dataset<Actor> actorsDataset;

    @Test
    public void testGetAllMovies() {
        // All movie info found in all databases with Movies (Redis and Mongo),
        movieDataset = movieService.getMovieList(null);
        movieDataset.show();
        // Redis contains only 2010 movies and is the only database containing certain attributes (originalTitle, isAdult, runtimeMinutes, startYear)
        // Performs a conceptual join and an entity reconstruction.
        Condition movieCondition = Condition.simple(MovieAttribute.startYear, Operator.EQUALS, 2010);
        movieDataset = movieService.getMovieList(movieCondition);
        movieDataset.show();
    }
    
    @Test
    public void testGetActorByMovieDirector(){
        //Complex And Condition
        AndCondition<DirectorAttribute> directorCondition = Condition.and(Condition.simple(DirectorAttribute.lastName,Operator.EQUALS,"Nolan"), Condition.simple(DirectorAttribute.firstName, Operator.EQUALS, "Christopher"));

        //Get Director
        Dataset<Director> directors = directorService.getDirectorList(directorCondition);
        Director nolan = directors.collectAsList().get(0);
        // Get Movie of role 'directed_movie' in relationship 'movieDirector' based on Director
        Dataset<Movie> movies = movieService.getMovieList(Movie.movieDirector.directed_movie, nolan);
        movies.show();
        Set<Actor> actorSet = new HashSet<>();
        movies.collectAsList().forEach(m -> {
            m._setCharacterList(actorService.getActorList(Actor.movieActor.character,m).collectAsList());
            actorSet.addAll(m._getCharacterList());
        });
        actorSet.forEach(System.out::println);

    }

}
