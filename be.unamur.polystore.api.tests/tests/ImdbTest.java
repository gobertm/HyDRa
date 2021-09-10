package generated;

import conditions.*;
import dao.impl.ActorServiceImpl;
import dao.impl.DirectorServiceImpl;
import dao.impl.MovieServiceImpl;
import dao.services.ActorService;
import dao.services.DirectorService;
import dao.services.MovieService;
import loading.Loading;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.spark.sql.Dataset;
import org.junit.Test;
import pojo.Actor;
import pojo.Director;
import pojo.Movie;

import java.util.HashSet;
import java.util.Set;

public class ImdbTest {

    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ImdbTest.class);
    DirectorService directorService = new DirectorServiceImpl();
    Dataset<Director> directorDataset;
    MovieService movieService = new MovieServiceImpl();
    Dataset<Movie> movieDataset;
    ActorService actorService = new ActorServiceImpl();
    Dataset<Actor> actors;


    @Test
    public void testActor(){
        Condition  condition =  null;
//        SimpleCondition<ActorAttribute> lastnameCon = new SimpleCondition<>(ActorAttribute.lastName,Operator.EQUALS,"Chandler");
//        SimpleCondition<ActorAttribute> firstnameCon = new SimpleCondition<>(ActorAttribute.firstName,Operator.EQUALS,"Kyle");
//        AndCondition condition = new AndCondition(lastnameCon,firstnameCon);
//        SimpleCondition<ActorAttribute> condition = new SimpleCondition<>(ActorAttribute.id,Operator.EQUALS,803);
        actors = actorService.getActorList(condition);
        actors.show();
    }

    @Test
    public void testMovie() {
//        SimpleCondition<MovieAttribute> condition = new SimpleCondition<>(MovieAttribute.id, Operator.EQUALS, "tt0000886");
//        SimpleCondition<MovieAttribute> condition = new SimpleCondition<>(MovieAttribute.primaryTitle, Operator.EQUALS, "Ocean's Eleven");
//        movieDataset = movieService.getMovieList(condition);
//        movieDataset2 = movieService.getMovieList(condition);
        movieDataset = movieService.getMovieList(null);
        logger.info("before count");
        movieDataset.show();
        System.out.println(movieDataset.count());
        logger.info("after");
    }

    @Test
    public void testDirector(){
        directorDataset = directorService.getDirectorList(Condition.simple(DirectorAttribute.id, Operator.EQUALS, "nm0304098"));
        directorDataset.show();

    }


    @Test
    public void testMovieByDirector() {
        directorDataset = directorService.getDirectorList(Condition.simple(DirectorAttribute.id, Operator.EQUALS, "nm0304098"));
        Director abelglance = directorDataset.collectAsList().get(0);
        movieDataset = movieService.getDirected_movieListInMovieDirectorByDirector(abelglance);
        movieDataset.show();
    }

    @Test
    public void testActorByMovie(){
        Movie movie = movieService.getMovieListByPrimaryTitle("Star Wars: Episode I - The Phantom Menace").first();
        System.out.println(movie);
        actors = actorService.getCharacterListInMovieActorByMovie(movie);
        actors.show();
    }

    @Test
    public void testMovieByActor(){
        Actor actor = actorService.getActorListByFullName("Keanu Reeves").first();
        movieDataset = movieService.getMovieListInMovieActorByCharacter(actor);
        movieDataset.show();
    }

    @Test
    public void testHybridCondition(){
        // rate in mongo, runtimeminutes in redis
        AndCondition<MovieAttribute> cond = Condition.and(Condition.simple(MovieAttribute.averageRating, Operator.GT, 8), Condition.simple(MovieAttribute.runtimeMinutes, Operator.GT, 120));
        movieDataset = movieService.getMovieList(cond);
        movieDataset.show();
    }

    @Test
    public void testActorByMovieByDirector(){
//        movieService.updateLoadingParameter(MovieService.ROLE_NAME.MOVIEACTOR_MOVIE,Loading.EAGER);   // Cette ligne permettrait de garnir movie.characterList lors du chargement des Movie.

        // Hybridation de la condition AND ? ex: lastname -> SQL, firstname -> Mongo..
        AndCondition<DirectorAttribute> condition = Condition.and(
                Condition.simple(DirectorAttribute.lastName,Operator.EQUALS,"Spielberg"),
                Condition.simple(DirectorAttribute.firstName, Operator.EQUALS, "Steven"));
        Dataset<Director> directors = directorService.getDirectorList(condition);
        Director spielberg = directors.collectAsList().get(0);
//        movieService.getDirected_movieListInMovieDirector(movieCondition, directorCondition);
        Dataset<Movie> movies = movieService.getDirected_movieListInMovieDirectorByDirector(spielberg); // getMovies(director, Director.ROLE.directed_movie)
        movies.show();
        Set<Actor> actorSet = new HashSet<>(); // TODO .equals des POJO à implémenter.
        movies.collectAsList().forEach(m -> {
            m._setCharacterList(actorService.getCharacterListInMovieActorByMovie(m).collectAsList()); // getActor(m , Actor.ROLE.Character);
            actorSet.addAll(m._getCharacterList());
        });
        actorSet.forEach(System.out::println);
    }

    @Test
    public void testUseCasePaper(){
        AndCondition<DirectorAttribute> directorCondition = Condition.and(
                Condition.simple(DirectorAttribute.lastName,Operator.EQUALS,"Spielberg"),
                Condition.simple(DirectorAttribute.firstName, Operator.EQUALS, "Steven"));
        Dataset<Director> directors = directorService.getDirectorList(directorCondition);
        Director spielberg = directors.collectAsList().get(0);
        Dataset<Movie> movies = movieService.getMovieList(Movie.movieDirector.directed_movie, spielberg); // getMovies(director, Director.ROLE.directed_movie)
        movies.show();
        Set<Actor> actorSet = new HashSet<>();
        movies.collectAsList().forEach(m -> {
            m._setCharacterList(actorService.getActorList(Actor.movieActor.character,m).collectAsList()); // getActor(m , Actor.ROLE.Character);
            actorSet.addAll(m._getCharacterList());
        });
        actorSet.forEach(System.out::println);

    }

    @Test
    public void testGetByRoleConditions() {
        AndCondition<DirectorAttribute> conditionSpielberg = Condition.and(
                Condition.simple(DirectorAttribute.lastName,Operator.EQUALS,"Spielberg"),
                Condition.simple(DirectorAttribute.firstName, Operator.EQUALS, "Steven"));

        //  Movie by Director and Movie Conditions
        Condition movieCondition = Condition.simple(MovieAttribute.startYear, Operator.GT, 2008);
        Dataset<Movie> movies = movieService.getMovieList(Movie.movieDirector.directed_movie, movieCondition, conditionSpielberg);
        movies.show();
    }

    @Test
    public void testMovieById(){
        Dataset<Movie> movie = movieService.getMovieList(Condition.simple(MovieAttribute.id, Operator.EQUALS, "tt0234215"));
        movie.show();
        movieService.getMovieListByPrimaryTitle("The Big Lebowski");
    }

//    @Test
//    public void testInsert(){
//        Movie movie = null;
//        movieService.insertMovie(movie);
//        movieService.insertMovieAndLinkedItems(movie);
//    }

}

