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

public class IMDBTestsPaper {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ImdbTest.class);
    DirectorService directorService = new DirectorServiceImpl();
    Dataset<Director> directorDataset;
    MovieService movieService = new MovieServiceImpl();
    Dataset<Movie> movieDataset;
    ActorService actorService = new ActorServiceImpl();
    Dataset<Actor> actorsDataset;

    @Test
    public void testGetActorByMovieDirector(){
        //Simple condition
        Condition movieCondition = Condition.simple(MovieAttribute.startYear, Operator.GT, 2008);
        //Complex And Condition
        AndCondition<DirectorAttribute> directorCondition = Condition.and(Condition.simple(DirectorAttribute.lastName,Operator.EQUALS,"Spielberg"), Condition.simple(DirectorAttribute.firstName, Operator.EQUALS, "Steven"));

        //Get Director
        Dataset<Director> directors = directorService.getDirectorList(directorCondition);
        Director spielberg = directors.collectAsList().get(0);
        // Get Movie of role 'directed_movie' in relationship 'movieDirector' based on Director
        Dataset<Movie> movies = movieService.getMovieList(Movie.movieDirector.directed_movie, spielberg);
//        movies.show();
        Set<Actor> actorSet = new HashSet<>();
        movies.collectAsList().forEach(m -> {
            m._setCharacterList(actorService.getActorList(Actor.movieActor.character,m).collectAsList());
            actorSet.addAll(m._getCharacterList());
        });
    }


}
