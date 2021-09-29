package dao.services;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import conditions.Condition;
import pojo.MovieActor;
import tdo.ActorTDO;
import tdo.MovieActorTDO;
import pojo.Actor;
import pojo.MovieActor;
import conditions.ActorAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;
import tdo.MovieTDO;
import tdo.MovieActorTDO;
import pojo.Movie;
import pojo.MovieActor;
import conditions.MovieAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;


public abstract class MovieActorService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MovieActorService.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	
	// method accessing the embedded object movies mapped to role character
	public abstract Dataset<pojo.MovieActor> getMovieActorListInIMDB_MongoactorCollectionmovies(Condition<ActorAttribute> character_condition, Condition<MovieAttribute> movie_condition, MutableBoolean character_refilter, MutableBoolean movie_refilter);
	
	// method accessing the embedded object actors mapped to role movie
	public abstract Dataset<pojo.MovieActor> getMovieActorListInIMDB_MongoreviewColmovieactors(Condition<MovieAttribute> movie_condition, Condition<ActorAttribute> character_condition, MutableBoolean movie_refilter, MutableBoolean character_refilter);
	// method accessing the embedded object actors mapped to role movie
	public abstract Dataset<pojo.MovieActor> getMovieActorListInIMDB_MongomovieColactors(Condition<MovieAttribute> movie_condition, Condition<ActorAttribute> character_condition, MutableBoolean movie_refilter, MutableBoolean character_refilter);
	
	
	
	public abstract java.util.List<pojo.MovieActor> getMovieActorList(
		conditions.Condition<conditions.ActorAttribute> character_condition,
		conditions.Condition<conditions.MovieAttribute> movie_condition);
	
	public java.util.List<pojo.MovieActor> getMovieActorListByCharacterCondition(
		conditions.Condition<conditions.ActorAttribute> character_condition
	){
		return getMovieActorList(character_condition, null);
	}
	
	public java.util.List<pojo.MovieActor> getMovieActorListByCharacter(pojo.Actor character) {
		// TODO using id for selecting
		return null;
	}
	public java.util.List<pojo.MovieActor> getMovieActorListByMovieCondition(
		conditions.Condition<conditions.MovieAttribute> movie_condition
	){
		return getMovieActorList(null, movie_condition);
	}
	
	public java.util.List<pojo.MovieActor> getMovieActorListByMovie(pojo.Movie movie) {
		// TODO using id for selecting
		return null;
	}
	
	public abstract void insertMovieActor(MovieActor movieActor);
	
	public abstract void deleteMovieActorList(
		conditions.Condition<conditions.ActorAttribute> character_condition,
		conditions.Condition<conditions.MovieAttribute> movie_condition);
	
	public void deleteMovieActorListByCharacterCondition(
		conditions.Condition<conditions.ActorAttribute> character_condition
	){
		deleteMovieActorList(character_condition, null);
	}
	
	public void deleteMovieActorListByCharacter(pojo.Actor character) {
		// TODO using id for selecting
		return;
	}
	public void deleteMovieActorListByMovieCondition(
		conditions.Condition<conditions.MovieAttribute> movie_condition
	){
		deleteMovieActorList(null, movie_condition);
	}
	
	public void deleteMovieActorListByMovie(pojo.Movie movie) {
		// TODO using id for selecting
		return;
	}
		
}
