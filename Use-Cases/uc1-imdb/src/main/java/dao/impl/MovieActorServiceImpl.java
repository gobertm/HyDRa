package dao.impl;

import exceptions.PhysicalStructureException;
import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import conditions.Condition;
import java.util.HashSet;
import java.util.Set;
import conditions.AndCondition;
import conditions.OrCondition;
import conditions.SimpleCondition;
import conditions.MovieActorAttribute;
import conditions.Operator;
import pojo.MovieActor;
import tdo.ActorTDO;
import tdo.MovieActorTDO;
import pojo.Actor;
import conditions.ActorAttribute;
import tdo.MovieTDO;
import tdo.MovieActorTDO;
import pojo.Movie;
import conditions.MovieAttribute;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.commons.lang.mutable.MutableBoolean;
import util.Util;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Row;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import scala.collection.mutable.WrappedArray;
import org.apache.spark.api.java.function.FlatMapFunction;
import dbconnection.SparkConnectionMgr;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.ArrayType;
import static com.mongodb.client.model.Updates.addToSet;


public class MovieActorServiceImpl extends dao.services.MovieActorService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MovieActorServiceImpl.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	
	// method accessing the embedded object movies mapped to role character
	public Dataset<pojo.MovieActor> getMovieActorListInIMDB_MongoactorCollectionmovies(Condition<ActorAttribute> character_condition, Condition<MovieAttribute> movie_condition, MutableBoolean character_refilter, MutableBoolean movie_refilter){	
			List<String> bsons = new ArrayList<String>();
			String bson = null;
			bson = ActorServiceImpl.getBSONMatchQueryInActorCollectionFromMymongo(character_condition ,character_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
			bson = MovieServiceImpl.getBSONMatchQueryInActorCollectionFromMymongo(movie_condition ,movie_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
		
			String bsonQuery = bsons.size() == 0 ? null : "{$match: { $and: [" + String.join(",", bsons) + "] }}";
		
			Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mymongo", "actorCollection", bsonQuery);
		
			Dataset<MovieActor> res = dataset.flatMap((FlatMapFunction<Row, MovieActor>) r -> {
					List<MovieActor> list_res = new ArrayList<MovieActor>();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					Row nestedRow = null;
		
					boolean addedInList = false;
					Row r1 = r;
					MovieActor movieActor1 = new MovieActor();
					movieActor1.setCharacter(new Actor());
					movieActor1.setMovie(new Movie());
					
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Actor.id for field id			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("id")) {
						if(nestedRow.getAs("id")==null)
							movieActor1.getCharacter().setId(null);
						else{
							movieActor1.getCharacter().setId((String)nestedRow.getAs("id"));
							toAdd1 = true;					
							}
					}
					// 	attribute Actor.fullName for field fullname			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("fullname")) {
						if(nestedRow.getAs("fullname")==null)
							movieActor1.getCharacter().setFullName(null);
						else{
							movieActor1.getCharacter().setFullName((String)nestedRow.getAs("fullname"));
							toAdd1 = true;					
							}
					}
					// 	attribute Actor.yearOfBirth for field birthyear			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("birthyear")) {
						if(nestedRow.getAs("birthyear")==null)
							movieActor1.getCharacter().setYearOfBirth(null);
						else{
							movieActor1.getCharacter().setYearOfBirth((String)nestedRow.getAs("birthyear"));
							toAdd1 = true;					
							}
					}
					// 	attribute Actor.yearOfDeath for field deathyear			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("deathyear")) {
						if(nestedRow.getAs("deathyear")==null)
							movieActor1.getCharacter().setYearOfDeath(null);
						else{
							movieActor1.getCharacter().setYearOfDeath((String)nestedRow.getAs("deathyear"));
							toAdd1 = true;					
							}
					}
					array1 = r1.getAs("movies");
					if(array1!= null) {
						for (int i2 = 0; i2 < array1.size(); i2++){
							Row r2 = (Row) array1.apply(i2);
							MovieActor movieActor2 = (MovieActor) movieActor1.clone();
							boolean toAdd2  = false;
							WrappedArray array2  = null;
							// 	attribute Movie.id for field id			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("id")) {
								if(nestedRow.getAs("id")==null)
									movieActor2.getMovie().setId(null);
								else{
									movieActor2.getMovie().setId((String)nestedRow.getAs("id"));
									toAdd2 = true;					
									}
							}
							// 	attribute Movie.primaryTitle for field title			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("title")) {
								if(nestedRow.getAs("title")==null)
									movieActor2.getMovie().setPrimaryTitle(null);
								else{
									movieActor2.getMovie().setPrimaryTitle((String)nestedRow.getAs("title"));
									toAdd2 = true;					
									}
							}
							// 	attribute Movie.averageRating for field rate			
							nestedRow =  r2;
							nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("rating");
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("rate")) {
								regex = "(.*)(/10)";
								groupIndex = 1;
								if(groupIndex == null) {
									throw new Exception("Cannot retrieve value for Movie.averageRating attribute stored in db mymongo. Probably due to an ambiguous regex.");
								}
								value = nestedRow.getAs("rate");
								p = Pattern.compile(regex);
								m = p.matcher(value);
								matches = m.find();
								if(matches) {
									String averageRating = m.group(groupIndex.intValue());
									movieActor2.getMovie().setAverageRating(averageRating == null ? null : averageRating);
									toAdd2 = true;
								} else {
									throw new Exception("Cannot retrieve value for Movie.averageRating attribute stored in db mymongo. Probably due to an ambiguous regex.");
								}
							}
							// 	attribute Movie.numVotes for field numberofvotes			
							nestedRow =  r2;
							nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("rating");
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("numberofvotes")) {
								if(nestedRow.getAs("numberofvotes")==null)
									movieActor2.getMovie().setNumVotes(null);
								else{
									movieActor2.getMovie().setNumVotes((Integer)nestedRow.getAs("numberofvotes"));
									toAdd2 = true;					
									}
							}
							if(toAdd2) {
								if((character_condition == null || character_refilter.booleanValue() || character_condition.evaluate(movieActor2.getCharacter()))&&(movie_condition == null || movie_refilter.booleanValue() || movie_condition.evaluate(movieActor2.getMovie())))
								list_res.add(movieActor2);
								addedInList = true;
							} 
							if(addedInList)
								toAdd1 = false;
						}
					}
					
					if(toAdd1) {
						
							list_res.add(movieActor1);
						addedInList = true;
					} 
					
					
					
					return list_res.iterator();
		
			}, Encoders.bean(MovieActor.class));
			// TODO drop duplicates based on roles ids
			//res= res.dropDuplicates(new String {});
			return res;
	}
	
	
	
	
	public java.util.List<pojo.MovieActor> getMovieActorList(
		conditions.Condition<conditions.ActorAttribute> character_condition,
		conditions.Condition<conditions.MovieAttribute> movie_condition){
			//TODO
			return null;
		}
	
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
	
	public void insertMovieActor(MovieActor movieActor){
		//TODO
	}
	
	public void deleteMovieActorList(
		conditions.Condition<conditions.ActorAttribute> character_condition,
		conditions.Condition<conditions.MovieAttribute> movie_condition){
			//TODO
		}
	
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
