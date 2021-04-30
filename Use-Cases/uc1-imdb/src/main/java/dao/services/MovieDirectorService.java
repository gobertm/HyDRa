package dao.services;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import conditions.Condition;
import pojo.MovieDirector;
import tdo.MovieTDO;
import tdo.MovieDirectorTDO;
import pojo.Movie;
import conditions.MovieAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;
import tdo.DirectorTDO;
import tdo.MovieDirectorTDO;
import pojo.Director;
import conditions.DirectorAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;


public abstract class MovieDirectorService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MovieDirectorService.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	
	
	//join structure
	// Left side 'movie_id' of reference [movie_info ]
	public abstract Dataset<MovieTDO> getMovieTDOListDirected_movieInMovie_infoInActorCollectionFromIMDB_Mongo(Condition<MovieAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	
	//join structure
	// Left side 'movie_id' of reference [has_directed ]
	public abstract Dataset<MovieTDO> getMovieTDOListDirected_movieInHas_directedInMovieKVFromMovieRedis(Condition<MovieAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	
	public abstract Dataset<MovieDirectorTDO> getMovieDirectorTDOListIndirectorTableAnddirectedFrommydb(Condition<DirectorAttribute> director_cond, MutableBoolean refilterFlag);
	
	
	
	
	
	
	
	
	public abstract java.util.List<pojo.MovieDirector> getMovieDirectorList(
		conditions.Condition<conditions.MovieAttribute> directed_movie_condition,
		conditions.Condition<conditions.DirectorAttribute> director_condition);
	
	public java.util.List<pojo.MovieDirector> getMovieDirectorListByDirected_movieCondition(
		conditions.Condition<conditions.MovieAttribute> directed_movie_condition
	){
		return getMovieDirectorList(directed_movie_condition, null);
	}
	
	public java.util.List<pojo.MovieDirector> getMovieDirectorListByDirected_movie(pojo.Movie directed_movie) {
		// TODO using id for selecting
		return null;
	}
	public java.util.List<pojo.MovieDirector> getMovieDirectorListByDirectorCondition(
		conditions.Condition<conditions.DirectorAttribute> director_condition
	){
		return getMovieDirectorList(null, director_condition);
	}
	
	public java.util.List<pojo.MovieDirector> getMovieDirectorListByDirector(pojo.Director director) {
		// TODO using id for selecting
		return null;
	}
	
	public abstract void insertMovieDirectorAndLinkedItems(pojo.MovieDirector movieDirector);
	
	public abstract void attachPersistentItemsByMovieDirector(
		pojo.Movie directed_movie,
		pojo.Director director);
	
	
	public abstract void deleteMovieDirectorList(
		conditions.Condition<conditions.MovieAttribute> directed_movie_condition,
		conditions.Condition<conditions.DirectorAttribute> director_condition);
	
	public void deleteMovieDirectorListByDirected_movieCondition(
		conditions.Condition<conditions.MovieAttribute> directed_movie_condition
	){
		deleteMovieDirectorList(directed_movie_condition, null);
	}
	
	public void deleteMovieDirectorListByDirected_movie(pojo.Movie directed_movie) {
		// TODO using id for selecting
		return;
	}
	public void deleteMovieDirectorListByDirectorCondition(
		conditions.Condition<conditions.DirectorAttribute> director_condition
	){
		deleteMovieDirectorList(null, director_condition);
	}
	
	public void deleteMovieDirectorListByDirector(pojo.Director director) {
		// TODO using id for selecting
		return;
	}
		
}
