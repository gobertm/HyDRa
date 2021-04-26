package dao.impl;

import java.util.Arrays;
import java.util.List;
import pojo.Movie;
import conditions.*;
import dao.services.MovieService;
import util.Util;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.function.MapFunction;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import com.mongodb.spark.MongoSpark;
import org.bson.Document;
import static java.util.Collections.singletonList;
import dbconnection.SparkConnectionMgr;
import dbconnection.DBConnectionMgr;
import scala.collection.mutable.WrappedArray;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FilterFunction;
import java.util.ArrayList;
import org.apache.commons.lang.mutable.MutableBoolean;
import tdo.*;
import pojo.*;

public class MovieServiceImpl extends MovieService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MovieServiceImpl.class);
	
	
	
	
	//TODO redis
	public Dataset<Movie> getMovieListInMovieKVFromMyredis(conditions.Condition<conditions.MovieAttribute> condition, MutableBoolean refilterFlag){
		// As we cannot filter on the values in a Redis DB, we always put the refilterflag to true.
		refilterFlag.setValue(true);
		// Build the key pattern
		//  - If the condition attribute is in the key pattern, replace by the value. Only if operator is EQUALS.
		//  - Replace all other fields of key pattern by a '*' 
		String keypattern= "";
		String finalKeypattern;
		List<String> fieldsListInKey = new ArrayList<>();
		keypattern=keypattern.concat("movie:");
		keypattern=keypattern.concat("*");
		fieldsListInKey.add("id");
			
		// Find the type of query to perform in order to retrieve a Dataset<Row>
		// Based on the type of the value. Is a it a simple string or a hash or a list... 
		Dataset<Row> rows;
			// TODO only handles 'hash' for now
		rows = SparkConnectionMgr.getRowsFromKeyValueHashes("myredis",keypattern);
		finalKeypattern = keypattern;
		Dataset<Movie> res = rows.map((MapFunction<Row, Movie>) r -> {
					Movie movie_res = new Movie();
					Integer groupindex = null;
					String regex = null;
					String value = null;
					Pattern p, pattern = null;
					Matcher m, match = null;
					boolean matches = false;
					// attribute [Movie.Id]
					// Attribute mapped in a key.
					String key = r.getAs("id");
					// Spark Redis automatically strips leading character if the pattern provided contains a single '*' at the end.				
					pattern = Pattern.compile("\\*");
			        match = pattern.matcher(finalKeypattern);
			        if(match.results().count()==1){
						movie_res.setId(key);
					}else{
						regex = finalKeypattern.replaceAll("\\*","(.*)");
						groupindex = fieldsListInKey.indexOf("id")+1;
						if(groupindex==null) {
							logger.warn("Attribute 'Movie' mapped physical field 'id' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
						}
						p = Pattern.compile(regex);
						m = p.matcher(key);
						matches = m.find();
						String id = null;
						if(matches) {
							id = m.group(groupindex.intValue());
						} else {
							logger.warn("Cannot retrieve value for Movieid attribute stored in db myredis. Probably due to an ambiguous regex.");
							throw new Exception("Cannot retrieve value for Movieid attribute stored in db myredis. Probably due to an ambiguous regex.");
						}
						movie_res.setId(id);
					}
					// attribute [Movie.PrimaryTitle]
					String primaryTitle = r.getAs("title")==null ? null : r.getAs("title");
					movie_res.setPrimaryTitle(primaryTitle);
					// attribute [Movie.OriginalTitle]
					String originalTitle = r.getAs("originalTitle")==null ? null : r.getAs("originalTitle");
					movie_res.setOriginalTitle(originalTitle);
					// attribute [Movie.IsAdult]
					Boolean isAdult = r.getAs("isAdult")==null ? null : Boolean.parseBoolean(r.getAs("isAdult"));
					movie_res.setIsAdult(isAdult);
					// attribute [Movie.StartYear]
					Integer startYear = r.getAs("startYear")==null ? null : Integer.parseInt(r.getAs("startYear"));
					movie_res.setStartYear(startYear);
					// attribute [Movie.RuntimeMinutes]
					Integer runtimeMinutes = r.getAs("runtimeMinutes")==null ? null : Integer.parseInt(r.getAs("runtimeMinutes"));
					movie_res.setRuntimeMinutes(runtimeMinutes);
	
						return movie_res;
				}, Encoders.bean(Movie.class));
		if(refilterFlag.booleanValue())
			res = res.filter((FilterFunction<Movie>) r -> condition == null || condition.evaluate(r));
		res=res.dropDuplicates();
		return res;
	
		
	}
	
	
	public static String getBSONMatchQueryInActorCollectionFromMymongo(Condition<MovieAttribute> condition, MutableBoolean refilterFlag) {	
		String res = null;	
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				MovieAttribute attr = ((SimpleCondition<MovieAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<MovieAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<MovieAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == MovieAttribute.id ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "id': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "movies." + res;
					res = "'" + res;
					}
					if(attr == MovieAttribute.primaryTitle ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "title': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "movies." + res;
					res = "'" + res;
					}
					if(attr == MovieAttribute.averageRating ) {
						isConditionAttrEncountered = true;
					
					String preparedValue = "@VAR@/10";
					boolean like_op = false;
					boolean not_like = false; 
					if(op == Operator.EQUALS && valueString != null && preparedValue.contains("@OTHERVAR@")) {
						//ex: @@VAR@@' '@@OTHERVAR@@"=> more than one vars in LongField: we shall use regex
						like_op = true;
						preparedValue = "@VAR@/10";
					} else {
						if(op == Operator.NOT_EQUALS && valueString != null && preparedValue.contains("@OTHERVAR@")) {
							//ex: @@VAR@@' '@@OTHERVAR@@"=> more than one vars in LongField: we shall use regex
							not_like = true;
							like_op = true;
							preparedValue = "@VAR@/10";
						}
					}
					if(op == Operator.CONTAINS && valueString != null) {
						like_op = true;
						preparedValue = "@VAR@/10";
						preparedValue = preparedValue.replaceAll("@VAR@", ".*@VAR@.*");
					}
						
					if(like_op)
						valueString = Util.escapeReservedRegexMongo(valueString);
					preparedValue = preparedValue.replaceAll("@VAR@", valueString).replaceAll("@OTHERVAR@", ".*");
					
					if(valueString.equals(preparedValue)) // 5 <=> 5, the preparedValue is the same type as the original value
						preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
					else // 5 => 5*, the preparedValue became a string
						preparedValue = Util.getDelimitedMongoValue(String.class, preparedValue);
						
	
					String mongoOp = (like_op) ? "$regex" : op.getMongoDBOperator();
					//if not_like = true then we need to complement/negate the given regex
					res = "rate': {" + (!not_like ? (mongoOp + ": " + preparedValue) : ("$not: {" + mongoOp + ": " + preparedValue + "}")) + "}";
	
						res = "rating." + res;
						res = "movies." + res;
					res = "'" + res;
					}
					if(attr == MovieAttribute.numVotes ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "numberofvotes': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "rating." + res;
						res = "movies." + res;
					res = "'" + res;
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						res = "$expr: {$eq:[1,1]}";
					}
					
				}
			}
	
			if(condition instanceof AndCondition) {
				String bsonLeft = getBSONMatchQueryInActorCollectionFromMymongo(((AndCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInActorCollectionFromMymongo(((AndCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $and: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";
			}
	
			if(condition instanceof OrCondition) {
				String bsonLeft = getBSONMatchQueryInActorCollectionFromMymongo(((OrCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInActorCollectionFromMymongo(((OrCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $or: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";	
			}
	
			
	
			
		}
	
		return res;
	}
	
	public Dataset<Movie> getMovieListInActorCollectionFromMymongo(conditions.Condition<conditions.MovieAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = MovieServiceImpl.getBSONMatchQueryInActorCollectionFromMymongo(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mymongo", "actorCollection", bsonQuery);
	
		Dataset<Movie> res = dataset.flatMap((FlatMapFunction<Row, Movie>) r -> {
				List<Movie> list_res = new ArrayList<Movie>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				Movie movie1 = new Movie();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					array1 = r1.getAs("movies");
					if(array1!= null) {
						for (int i2 = 0; i2 < array1.size(); i2++){
							Row r2 = (Row) array1.apply(i2);
							Movie movie2 = (Movie) movie1.clone();
							boolean toAdd2  = false;
							WrappedArray array2  = null;
							// 	attribute Movie.id for field id			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("id")) {
								if(nestedRow.getAs("id")==null)
									movie2.setId(null);
								else{
									movie2.setId((String) nestedRow.getAs("id"));
									toAdd2 = true;					
									}
							}
							// 	attribute Movie.primaryTitle for field title			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("title")) {
								if(nestedRow.getAs("title")==null)
									movie2.setPrimaryTitle(null);
								else{
									movie2.setPrimaryTitle((String) nestedRow.getAs("title"));
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
									movie2.setAverageRating(Double.parseDouble(averageRating));
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
									movie2.setNumVotes(null);
								else{
									movie2.setNumVotes((Integer) nestedRow.getAs("numberofvotes"));
									toAdd2 = true;					
									}
							}
							if(toAdd2) {
								if(condition ==null || refilterFlag.booleanValue() || condition.evaluate(movie2))
								list_res.add(movie2);
								addedInList = true;
							} 
							if(addedInList)
								toAdd1 = false;
						}
					}
					
					if(toAdd1) {
						
							list_res.add(movie1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(Movie.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
		
	}
	
	
	// TODO get based on id(s). Ex:public Client getClientById(Long id)
	
	public Dataset<Movie> getMovieListById(String id) {
		return getMovieList(conditions.Condition.simple(conditions.MovieAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Movie> getMovieListByPrimaryTitle(String primaryTitle) {
		return getMovieList(conditions.Condition.simple(conditions.MovieAttribute.primaryTitle, conditions.Operator.EQUALS, primaryTitle));
	}
	
	public Dataset<Movie> getMovieListByOriginalTitle(String originalTitle) {
		return getMovieList(conditions.Condition.simple(conditions.MovieAttribute.originalTitle, conditions.Operator.EQUALS, originalTitle));
	}
	
	public Dataset<Movie> getMovieListByIsAdult(Boolean isAdult) {
		return getMovieList(conditions.Condition.simple(conditions.MovieAttribute.isAdult, conditions.Operator.EQUALS, isAdult));
	}
	
	public Dataset<Movie> getMovieListByStartYear(Integer startYear) {
		return getMovieList(conditions.Condition.simple(conditions.MovieAttribute.startYear, conditions.Operator.EQUALS, startYear));
	}
	
	public Dataset<Movie> getMovieListByRuntimeMinutes(Integer runtimeMinutes) {
		return getMovieList(conditions.Condition.simple(conditions.MovieAttribute.runtimeMinutes, conditions.Operator.EQUALS, runtimeMinutes));
	}
	
	public Dataset<Movie> getMovieListByAverageRating(Double averageRating) {
		return getMovieList(conditions.Condition.simple(conditions.MovieAttribute.averageRating, conditions.Operator.EQUALS, averageRating));
	}
	
	public Dataset<Movie> getMovieListByNumVotes(Integer numVotes) {
		return getMovieList(conditions.Condition.simple(conditions.MovieAttribute.numVotes, conditions.Operator.EQUALS, numVotes));
	}
	
	
	
	
	public Dataset<Movie> getDirected_movieListInMovieDirector(conditions.Condition<conditions.MovieAttribute> directed_movie_condition,conditions.Condition<conditions.DirectorAttribute> director_condition)		{
		MutableBoolean directed_movie_refilter = new MutableBoolean(false);
		List<Dataset<Movie>> datasetsPOJO = new ArrayList<Dataset<Movie>>();
		Dataset<Director> all = new DirectorServiceImpl().getDirectorList(director_condition);
		boolean all_already_persisted = false;
		MutableBoolean director_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// join physical structure
	
		//join between 2 SQL tables and a non-relational structure
		director_refilter = new MutableBoolean(false);
		Dataset<MovieDirectorTDO> res_movieDirector_directed_by_movie_info = movieDirectorService.getMovieDirectorTDOListIndirectorTableAnddirectedFrommydb(director_condition, director_refilter);
		if(director_refilter.booleanValue()) {
				joinCondition = null;
				joinCondition = res_movieDirector_directed_by_movie_info.col("director.id").equalTo(all.col("id"));
				res_movieDirector_directed_by_movie_info = res_movieDirector_directed_by_movie_info.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(MovieDirectorTDO.class));
		} 
		Dataset<MovieTDO> res_movie_info_directed_by = movieDirectorService.getMovieTDOListDirected_movieInMovie_infoInActorCollectionFromIMDB_Mongo(directed_movie_condition, directed_movie_refilter);
		Dataset<Row> res_row_directed_by_movie_info = res_movieDirector_directed_by_movie_info.join(res_movie_info_directed_by.withColumnRenamed("logEvents", "movieDirector_logEvents"),
			res_movie_info_directed_by.col("myRelSchema_directed_movie_info_id").equalTo(res_movieDirector_directed_by_movie_info.col("myRelSchema_directed_movie_info_movie_id")));
		Dataset<Movie> res_Movie_movie_info = res_row_directed_by_movie_info.as(Encoders.bean(Movie.class));
		datasetsPOJO.add(res_Movie_movie_info.dropDuplicates(new String[] {"id"}));	
		
		// join physical structure
	
		//join between 2 SQL tables and a non-relational structure
		director_refilter = new MutableBoolean(false);
		Dataset<MovieDirectorTDO> res_movieDirector_directed_by_has_directed = movieDirectorService.getMovieDirectorTDOListIndirectorTableAnddirectedFrommydb(director_condition, director_refilter);
		if(director_refilter.booleanValue()) {
				joinCondition = null;
				joinCondition = res_movieDirector_directed_by_has_directed.col("director.id").equalTo(all.col("id"));
				res_movieDirector_directed_by_has_directed = res_movieDirector_directed_by_has_directed.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(MovieDirectorTDO.class));
		} 
		Dataset<MovieTDO> res_has_directed_directed_by = movieDirectorService.getMovieTDOListDirected_movieInHas_directedInMovieKVFromMovieRedis(directed_movie_condition, directed_movie_refilter);
		Dataset<Row> res_row_directed_by_has_directed = res_movieDirector_directed_by_has_directed.join(res_has_directed_directed_by.withColumnRenamed("logEvents", "movieDirector_logEvents"),
			res_has_directed_directed_by.col("myRelSchema_directed_has_directed_id").equalTo(res_movieDirector_directed_by_has_directed.col("myRelSchema_directed_has_directed_movie_id")));
		Dataset<Movie> res_Movie_has_directed = res_row_directed_by_has_directed.as(Encoders.bean(Movie.class));
		datasetsPOJO.add(res_Movie_has_directed.dropDuplicates(new String[] {"id"}));	
		
		
		
		
		
		//Join datasets or return 
		Dataset<Movie> res = fullOuterJoinsMovie(datasetsPOJO);
		if(res == null)
			return null;
	
		if(directed_movie_refilter.booleanValue())
			res = res.filter((FilterFunction<Movie>) r -> directed_movie_condition == null || directed_movie_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Movie> getDirected_movieListInMovieDirectorByDirected_movieCondition(conditions.Condition<conditions.MovieAttribute> directed_movie_condition){
		return getDirected_movieListInMovieDirector(directed_movie_condition, null);
	}
	public Dataset<Movie> getDirected_movieListInMovieDirectorByDirectorCondition(conditions.Condition<conditions.DirectorAttribute> director_condition){
		return getDirected_movieListInMovieDirector(null, director_condition);
	}
	
	public Dataset<Movie> getDirected_movieListInMovieDirectorByDirector(pojo.Director director){
		if(director == null)
			return null;
	
		Condition c;
		c=Condition.simple(DirectorAttribute.id,Operator.EQUALS, director.getId());
		Dataset<Movie> res = getDirected_movieListInMovieDirectorByDirectorCondition(c);
		return res;
	}
	
	public Dataset<Movie> getMovieListInMovieActor(conditions.Condition<conditions.ActorAttribute> character_condition,conditions.Condition<conditions.MovieAttribute> movie_condition)		{
		MutableBoolean movie_refilter = new MutableBoolean(false);
		List<Dataset<Movie>> datasetsPOJO = new ArrayList<Dataset<Movie>>();
		Dataset<Actor> all = new ActorServiceImpl().getActorList(character_condition);
		boolean all_already_persisted = false;
		MutableBoolean character_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		
		// Role 'character' mapped to EmbeddedObject 'movies' 'Movie' containing 'Actor' 
		character_refilter = new MutableBoolean(false);
		Dataset<Movie> res_Movie;
		Dataset<MovieActor> res_movieActor_movie;
		res_movieActor_movie = movieActorService.getMovieActorListInIMDB_MongoactorCollectionmovies(character_condition, movie_condition, character_refilter, movie_refilter);
		if(character_refilter.booleanValue()) {
			joinCondition = null;
			joinCondition = res_movieActor_movie.col("character.id").equalTo(all.col("id"));
			if(joinCondition == null)
				res_Movie = res_movieActor_movie.join(all).select("movie.*").as(Encoders.bean(Movie.class));
			else
				res_Movie = res_movieActor_movie.join(all, joinCondition).select("movie.*").as(Encoders.bean(Movie.class));
		
		} else
			res_Movie = res_movieActor_movie.map((MapFunction<MovieActor,Movie>) r -> r.getMovie(), Encoders.bean(Movie.class));
		res_Movie = res_Movie.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Movie);
		
		
		//Join datasets or return 
		Dataset<Movie> res = fullOuterJoinsMovie(datasetsPOJO);
		if(res == null)
			return null;
	
		List<Dataset<Movie>> lonelyMovieList = new ArrayList<Dataset<Movie>>();
		lonelyMovieList.add(getMovieListInMovieKVFromMyredis(movie_condition, new MutableBoolean(false)));
		Dataset<Movie> lonelyMovie = fullOuterJoinsMovie(lonelyMovieList);
		if(lonelyMovie != null) {
			res = fullLeftOuterJoinsMovie(Arrays.asList(res, lonelyMovie));
		}
		if(movie_refilter.booleanValue())
			res = res.filter((FilterFunction<Movie>) r -> movie_condition == null || movie_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Movie> getMovieListInMovieActorByCharacterCondition(conditions.Condition<conditions.ActorAttribute> character_condition){
		return getMovieListInMovieActor(character_condition, null);
	}
	
	public Dataset<Movie> getMovieListInMovieActorByCharacter(pojo.Actor character){
		if(character == null)
			return null;
	
		Condition c;
		c=Condition.simple(ActorAttribute.id,Operator.EQUALS, character.getId());
		Dataset<Movie> res = getMovieListInMovieActorByCharacterCondition(c);
		return res;
	}
	
	public Dataset<Movie> getMovieListInMovieActorByMovieCondition(conditions.Condition<conditions.MovieAttribute> movie_condition){
		return getMovieListInMovieActor(null, movie_condition);
	}
	
	public void insertMovieAndLinkedItems(Movie movie){
		//TODO
	}
	public void insertMovie(Movie movie){
		// Insert into all mapped AbstractPhysicalStructure 
			insertMovieInMovieKVFromMyredis(movie);
			insertMovieInActorCollectionFromMymongo(movie);
	}
	
	public void insertMovieInMovieKVFromMyredis(Movie movie)	{
			//other databases to implement
		}
	public void insertMovieInActorCollectionFromMymongo(Movie movie){
		//Read mapping rules and find attributes of the POJO that are mapped to the corresponding AbstractPhysicalStructure
		// Insert in MongoDB
	}
	
	public void updateMovieList(conditions.Condition<conditions.MovieAttribute> condition, conditions.SetClause<conditions.MovieAttribute> set){
		//TODO
	}
	
	public void updateMovie(pojo.Movie movie) {
		//TODO using the id
		return;
	}
	public void updateDirected_movieListInMovieDirector(
		conditions.Condition<conditions.MovieAttribute> directed_movie_condition,
		conditions.Condition<conditions.DirectorAttribute> director_condition,
		
		conditions.SetClause<conditions.MovieAttribute> set
	){
		//TODO
	}
	
	public void updateDirected_movieListInMovieDirectorByDirected_movieCondition(
		conditions.Condition<conditions.MovieAttribute> directed_movie_condition,
		conditions.SetClause<conditions.MovieAttribute> set
	){
		updateDirected_movieListInMovieDirector(directed_movie_condition, null, set);
	}
	public void updateDirected_movieListInMovieDirectorByDirectorCondition(
		conditions.Condition<conditions.DirectorAttribute> director_condition,
		conditions.SetClause<conditions.MovieAttribute> set
	){
		updateDirected_movieListInMovieDirector(null, director_condition, set);
	}
	
	public void updateDirected_movieListInMovieDirectorByDirector(
		pojo.Director director,
		conditions.SetClause<conditions.MovieAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateMovieListInMovieActor(
		conditions.Condition<conditions.ActorAttribute> character_condition,
		conditions.Condition<conditions.MovieAttribute> movie_condition,
		
		conditions.SetClause<conditions.MovieAttribute> set
	){
		//TODO
	}
	
	public void updateMovieListInMovieActorByCharacterCondition(
		conditions.Condition<conditions.ActorAttribute> character_condition,
		conditions.SetClause<conditions.MovieAttribute> set
	){
		updateMovieListInMovieActor(character_condition, null, set);
	}
	
	public void updateMovieListInMovieActorByCharacter(
		pojo.Actor character,
		conditions.SetClause<conditions.MovieAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateMovieListInMovieActorByMovieCondition(
		conditions.Condition<conditions.MovieAttribute> movie_condition,
		conditions.SetClause<conditions.MovieAttribute> set
	){
		updateMovieListInMovieActor(null, movie_condition, set);
	}
	
	
	public void deleteMovieList(conditions.Condition<conditions.MovieAttribute> condition){
		//TODO
	}
	
	public void deleteMovie(pojo.Movie movie) {
		//TODO using the id
		return;
	}
	public void deleteDirected_movieListInMovieDirector(	
		conditions.Condition<conditions.MovieAttribute> directed_movie_condition,	
		conditions.Condition<conditions.DirectorAttribute> director_condition){
			//TODO
		}
	
	public void deleteDirected_movieListInMovieDirectorByDirected_movieCondition(
		conditions.Condition<conditions.MovieAttribute> directed_movie_condition
	){
		deleteDirected_movieListInMovieDirector(directed_movie_condition, null);
	}
	public void deleteDirected_movieListInMovieDirectorByDirectorCondition(
		conditions.Condition<conditions.DirectorAttribute> director_condition
	){
		deleteDirected_movieListInMovieDirector(null, director_condition);
	}
	
	public void deleteDirected_movieListInMovieDirectorByDirector(
		pojo.Director director 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteMovieListInMovieActor(	
		conditions.Condition<conditions.ActorAttribute> character_condition,	
		conditions.Condition<conditions.MovieAttribute> movie_condition){
			//TODO
		}
	
	public void deleteMovieListInMovieActorByCharacterCondition(
		conditions.Condition<conditions.ActorAttribute> character_condition
	){
		deleteMovieListInMovieActor(character_condition, null);
	}
	
	public void deleteMovieListInMovieActorByCharacter(
		pojo.Actor character 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteMovieListInMovieActorByMovieCondition(
		conditions.Condition<conditions.MovieAttribute> movie_condition
	){
		deleteMovieListInMovieActor(null, movie_condition);
	}
	
}
