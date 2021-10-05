package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Actor;
import conditions.*;
import dao.services.ActorService;
import util.Util;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.ArrayType;
import scala.Tuple2;
import org.bson.Document;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;


public class ActorServiceImpl extends ActorService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ActorServiceImpl.class);
	
	
	
	
	public static String getBSONMatchQueryInMovieColFromMymongo(Condition<ActorAttribute> condition, MutableBoolean refilterFlag) {	
		String res = null;	
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				ActorAttribute attr = ((SimpleCondition<ActorAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<ActorAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<ActorAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == ActorAttribute.id ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "actorid': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "actors." + res;
					res = "'" + res;
					}
					if(attr == ActorAttribute.fullName ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "name': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "actors." + res;
					res = "'" + res;
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						res = "$expr: {$eq:[1,1]}";
					}
					
				}
			}
	
			if(condition instanceof AndCondition) {
				String bsonLeft = getBSONMatchQueryInMovieColFromMymongo(((AndCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInMovieColFromMymongo(((AndCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $and: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";
			}
	
			if(condition instanceof OrCondition) {
				String bsonLeft = getBSONMatchQueryInMovieColFromMymongo(((OrCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInMovieColFromMymongo(((OrCondition)condition).getRightCondition(), refilterFlag);			
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
	
	public Dataset<Actor> getActorListInMovieColFromMymongo(conditions.Condition<conditions.ActorAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = ActorServiceImpl.getBSONMatchQueryInMovieColFromMymongo(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mymongo", "movieCol", bsonQuery);
	
		Dataset<Actor> res = dataset.flatMap((FlatMapFunction<Row, Actor>) r -> {
				List<Actor> list_res = new ArrayList<Actor>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				Actor actor1 = new Actor();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					array1 = r1.getAs("actors");
					if(array1!= null) {
						for (int i2 = 0; i2 < array1.size(); i2++){
							Row r2 = (Row) array1.apply(i2);
							Actor actor2 = (Actor) actor1.clone();
							boolean toAdd2  = false;
							WrappedArray array2  = null;
							// 	attribute Actor.id for field actorid			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("actorid")) {
								if(nestedRow.getAs("actorid")==null)
									actor2.setId(null);
								else{
									actor2.setId((String) nestedRow.getAs("actorid"));
									toAdd2 = true;					
									}
							}
							// 	attribute Actor.fullName for field name			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("name")) {
								if(nestedRow.getAs("name")==null)
									actor2.setFullName(null);
								else{
									actor2.setFullName((String) nestedRow.getAs("name"));
									toAdd2 = true;					
									}
							}
							if(toAdd2) {
								if(condition ==null || refilterFlag.booleanValue() || condition.evaluate(actor2))
								list_res.add(actor2);
								addedInList = true;
							} 
							if(addedInList)
								toAdd1 = false;
						}
					}
					
					if(toAdd1) {
						
							list_res.add(actor1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(Actor.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
		
	}
	
	
	public static String getBSONMatchQueryInReviewColFromMymongo(Condition<ActorAttribute> condition, MutableBoolean refilterFlag) {	
		String res = null;	
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				ActorAttribute attr = ((SimpleCondition<ActorAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<ActorAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<ActorAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == ActorAttribute.id ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "id': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "actors." + res;
						res = "movie." + res;
					res = "'" + res;
					}
					if(attr == ActorAttribute.fullName ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "name': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "actors." + res;
						res = "movie." + res;
					res = "'" + res;
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						res = "$expr: {$eq:[1,1]}";
					}
					
				}
			}
	
			if(condition instanceof AndCondition) {
				String bsonLeft = getBSONMatchQueryInReviewColFromMymongo(((AndCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInReviewColFromMymongo(((AndCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $and: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";
			}
	
			if(condition instanceof OrCondition) {
				String bsonLeft = getBSONMatchQueryInReviewColFromMymongo(((OrCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInReviewColFromMymongo(((OrCondition)condition).getRightCondition(), refilterFlag);			
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
	
	public Dataset<Actor> getActorListInReviewColFromMymongo(conditions.Condition<conditions.ActorAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = ActorServiceImpl.getBSONMatchQueryInReviewColFromMymongo(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mymongo", "reviewCol", bsonQuery);
	
		Dataset<Actor> res = dataset.flatMap((FlatMapFunction<Row, Actor>) r -> {
				List<Actor> list_res = new ArrayList<Actor>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				Actor actor1 = new Actor();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					if(toAdd1) {
						
						list_res.add(actor1);
						addedInList = true;
					} 
					
				
				return list_res.iterator();
	
		}, Encoders.bean(Actor.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
		
	}
	
	
	public static String getBSONMatchQueryInActorCollectionFromMymongo(Condition<ActorAttribute> condition, MutableBoolean refilterFlag) {	
		String res = null;	
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				ActorAttribute attr = ((SimpleCondition<ActorAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<ActorAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<ActorAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == ActorAttribute.id ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "id': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == ActorAttribute.fullName ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "fullname': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == ActorAttribute.yearOfBirth ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "birthyear': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == ActorAttribute.yearOfDeath ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "deathyear': {" + mongoOp + ": " + preparedValue + "}";
	
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
	
	public Dataset<Actor> getActorListInActorCollectionFromMymongo(conditions.Condition<conditions.ActorAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = ActorServiceImpl.getBSONMatchQueryInActorCollectionFromMymongo(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mymongo", "actorCollection", bsonQuery);
	
		Dataset<Actor> res = dataset.flatMap((FlatMapFunction<Row, Actor>) r -> {
				List<Actor> list_res = new ArrayList<Actor>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				Actor actor1 = new Actor();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Actor.id for field id			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("id")) {
						if(nestedRow.getAs("id")==null)
							actor1.setId(null);
						else{
							actor1.setId((String) nestedRow.getAs("id"));
							toAdd1 = true;					
							}
					}
					// 	attribute Actor.fullName for field fullname			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("fullname")) {
						if(nestedRow.getAs("fullname")==null)
							actor1.setFullName(null);
						else{
							actor1.setFullName((String) nestedRow.getAs("fullname"));
							toAdd1 = true;					
							}
					}
					// 	attribute Actor.yearOfBirth for field birthyear			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("birthyear")) {
						if(nestedRow.getAs("birthyear")==null)
							actor1.setYearOfBirth(null);
						else{
							actor1.setYearOfBirth((String) nestedRow.getAs("birthyear"));
							toAdd1 = true;					
							}
					}
					// 	attribute Actor.yearOfDeath for field deathyear			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("deathyear")) {
						if(nestedRow.getAs("deathyear")==null)
							actor1.setYearOfDeath(null);
						else{
							actor1.setYearOfDeath((String) nestedRow.getAs("deathyear"));
							toAdd1 = true;					
							}
					}
					if(toAdd1) {
						
						list_res.add(actor1);
						addedInList = true;
					} 
					
				
				return list_res.iterator();
	
		}, Encoders.bean(Actor.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
		
	}
	
	
	// TODO get based on id(s). Ex:public Client getClientById(Long id)
	
	public Dataset<Actor> getActorListById(String id) {
		return getActorList(conditions.Condition.simple(conditions.ActorAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Actor> getActorListByFullName(String fullName) {
		return getActorList(conditions.Condition.simple(conditions.ActorAttribute.fullName, conditions.Operator.EQUALS, fullName));
	}
	
	public Dataset<Actor> getActorListByYearOfBirth(String yearOfBirth) {
		return getActorList(conditions.Condition.simple(conditions.ActorAttribute.yearOfBirth, conditions.Operator.EQUALS, yearOfBirth));
	}
	
	public Dataset<Actor> getActorListByYearOfDeath(String yearOfDeath) {
		return getActorList(conditions.Condition.simple(conditions.ActorAttribute.yearOfDeath, conditions.Operator.EQUALS, yearOfDeath));
	}
	
	
	
	
	public Dataset<Actor> getCharacterListInMovieActor(conditions.Condition<conditions.ActorAttribute> character_condition,conditions.Condition<conditions.MovieAttribute> movie_condition)		{
		MutableBoolean character_refilter = new MutableBoolean(false);
		List<Dataset<Actor>> datasetsPOJO = new ArrayList<Dataset<Actor>>();
		Dataset<Movie> all = new MovieServiceImpl().getMovieList(movie_condition);
		boolean all_already_persisted = false;
		MutableBoolean movie_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		
		Dataset<MovieActor> res_movieActor_character;
		Dataset<Actor> res_Actor;
		// Role 'character' mapped to EmbeddedObject 'movies' - 'Movie' containing 'Actor'
		movie_refilter = new MutableBoolean(false);
		res_movieActor_character = movieActorService.getMovieActorListInIMDB_MongoactorCollectionmovies(character_condition, movie_condition, character_refilter, movie_refilter);
		if(movie_refilter.booleanValue()) {
			joinCondition = null;
			joinCondition = res_movieActor_character.col("movie.id").equalTo(all.col("id"));
			if(joinCondition == null)
				res_Actor = res_movieActor_character.join(all).select("character.*").as(Encoders.bean(Actor.class));
			else
				res_Actor = res_movieActor_character.join(all, joinCondition).select("character.*").as(Encoders.bean(Actor.class));
		
		} else
			res_Actor = res_movieActor_character.map((MapFunction<MovieActor,Actor>) r -> r.getCharacter(), Encoders.bean(Actor.class));
		res_Actor = res_Actor.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Actor);
		// Role 'movie' mapped to EmbeddedObject 'actors' 'Actor' containing 'Movie' 
		movie_refilter = new MutableBoolean(false);
		res_movieActor_character = movieActorService.getMovieActorListInIMDB_MongoreviewColmovieactors(movie_condition, character_condition, movie_refilter, character_refilter);
		if(movie_refilter.booleanValue()) {
			joinCondition = null;
			joinCondition = res_movieActor_character.col("movie.id").equalTo(all.col("id"));
			if(joinCondition == null)
				res_Actor = res_movieActor_character.join(all).select("character.*").as(Encoders.bean(Actor.class));
			else
				res_Actor = res_movieActor_character.join(all, joinCondition).select("character.*").as(Encoders.bean(Actor.class));
		
		} else
			res_Actor = res_movieActor_character.map((MapFunction<MovieActor,Actor>) r -> r.getCharacter(), Encoders.bean(Actor.class));
		res_Actor = res_Actor.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Actor);
		// Role 'movie' mapped to EmbeddedObject 'actors' 'Actor' containing 'Movie' 
		movie_refilter = new MutableBoolean(false);
		res_movieActor_character = movieActorService.getMovieActorListInIMDB_MongomovieColactors(movie_condition, character_condition, movie_refilter, character_refilter);
		if(movie_refilter.booleanValue()) {
			joinCondition = null;
			joinCondition = res_movieActor_character.col("movie.id").equalTo(all.col("id"));
			if(joinCondition == null)
				res_Actor = res_movieActor_character.join(all).select("character.*").as(Encoders.bean(Actor.class));
			else
				res_Actor = res_movieActor_character.join(all, joinCondition).select("character.*").as(Encoders.bean(Actor.class));
		
		} else
			res_Actor = res_movieActor_character.map((MapFunction<MovieActor,Actor>) r -> r.getCharacter(), Encoders.bean(Actor.class));
		res_Actor = res_Actor.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Actor);
		
		
		//Join datasets or return 
		Dataset<Actor> res = fullOuterJoinsActor(datasetsPOJO);
		if(res == null)
			return null;
	
		if(character_refilter.booleanValue())
			res = res.filter((FilterFunction<Actor>) r -> character_condition == null || character_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Actor> getCharacterListInMovieActorByCharacterCondition(conditions.Condition<conditions.ActorAttribute> character_condition){
		return getCharacterListInMovieActor(character_condition, null);
	}
	public Dataset<Actor> getCharacterListInMovieActorByMovieCondition(conditions.Condition<conditions.MovieAttribute> movie_condition){
		return getCharacterListInMovieActor(null, movie_condition);
	}
	
	public Dataset<Actor> getCharacterListInMovieActorByMovie(pojo.Movie movie){
		if(movie == null)
			return null;
	
		Condition c;
		c=Condition.simple(MovieAttribute.id,Operator.EQUALS, movie.getId());
		Dataset<Actor> res = getCharacterListInMovieActorByMovieCondition(c);
		return res;
	}
	
	
	public boolean insertActor(Actor actor){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertActorInActorCollectionFromMymongo(actor) || inserted ;
		return inserted;
	}
	
	public boolean insertActorInActorCollectionFromMymongo(Actor actor)	{
		Condition<ActorAttribute> conditionID;
		String idvalue="";
		conditionID = Condition.simple(ActorAttribute.id, Operator.EQUALS, actor.getId());
		idvalue+=actor.getId();
		boolean entityExists=false;
		entityExists = !getActorListInActorCollectionFromMymongo(conditionID,new MutableBoolean(false)).isEmpty();
				
		if(!entityExists){
		List<Row> listRows=new ArrayList<Row>();
		List<Object> valuesactorCollection_1 = new ArrayList<>();
		List<StructField> listOfStructFieldactorCollection_1 = new ArrayList<StructField>();
		if(!listOfStructFieldactorCollection_1.contains(DataTypes.createStructField("id",DataTypes.StringType, true)))
			listOfStructFieldactorCollection_1.add(DataTypes.createStructField("id",DataTypes.StringType, true));
		valuesactorCollection_1.add(actor.getId());
		if(!listOfStructFieldactorCollection_1.contains(DataTypes.createStructField("fullname",DataTypes.StringType, true)))
			listOfStructFieldactorCollection_1.add(DataTypes.createStructField("fullname",DataTypes.StringType, true));
		valuesactorCollection_1.add(actor.getFullName());
		if(!listOfStructFieldactorCollection_1.contains(DataTypes.createStructField("birthyear",DataTypes.StringType, true)))
			listOfStructFieldactorCollection_1.add(DataTypes.createStructField("birthyear",DataTypes.StringType, true));
		valuesactorCollection_1.add(actor.getYearOfBirth());
		if(!listOfStructFieldactorCollection_1.contains(DataTypes.createStructField("deathyear",DataTypes.StringType, true)))
			listOfStructFieldactorCollection_1.add(DataTypes.createStructField("deathyear",DataTypes.StringType, true));
		valuesactorCollection_1.add(actor.getYearOfDeath());
		
		StructType struct = DataTypes.createStructType(listOfStructFieldactorCollection_1);
		listRows.add(RowFactory.create(valuesactorCollection_1.toArray()));
		SparkConnectionMgr.writeDataset(listRows, struct, "mongo", "actorCollection", "mymongo");
			logger.info("Inserted [Actor] entity ID [{}] in [ActorCollection] in database [Mymongo]", idvalue);
		}
		else
			logger.warn("[Actor] entity ID [{}] already present in [ActorCollection] in database [Mymongo]", idvalue);
		return !entityExists;
	} 
	
	
	public void updateActorList(conditions.Condition<conditions.ActorAttribute> condition, conditions.SetClause<conditions.ActorAttribute> set){
		//TODO
	}
	
	public void updateActor(pojo.Actor actor) {
		//TODO using the id
		return;
	}
	public void updateCharacterListInMovieActor(
		conditions.Condition<conditions.ActorAttribute> character_condition,
		conditions.Condition<conditions.MovieAttribute> movie_condition,
		
		conditions.SetClause<conditions.ActorAttribute> set
	){
		//TODO
	}
	
	public void updateCharacterListInMovieActorByCharacterCondition(
		conditions.Condition<conditions.ActorAttribute> character_condition,
		conditions.SetClause<conditions.ActorAttribute> set
	){
		updateCharacterListInMovieActor(character_condition, null, set);
	}
	public void updateCharacterListInMovieActorByMovieCondition(
		conditions.Condition<conditions.MovieAttribute> movie_condition,
		conditions.SetClause<conditions.ActorAttribute> set
	){
		updateCharacterListInMovieActor(null, movie_condition, set);
	}
	
	public void updateCharacterListInMovieActorByMovie(
		pojo.Movie movie,
		conditions.SetClause<conditions.ActorAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	
	
	public void deleteActorList(conditions.Condition<conditions.ActorAttribute> condition){
		//TODO
	}
	
	public void deleteActor(pojo.Actor actor) {
		//TODO using the id
		return;
	}
	public void deleteCharacterListInMovieActor(	
		conditions.Condition<conditions.ActorAttribute> character_condition,	
		conditions.Condition<conditions.MovieAttribute> movie_condition){
			//TODO
		}
	
	public void deleteCharacterListInMovieActorByCharacterCondition(
		conditions.Condition<conditions.ActorAttribute> character_condition
	){
		deleteCharacterListInMovieActor(character_condition, null);
	}
	public void deleteCharacterListInMovieActorByMovieCondition(
		conditions.Condition<conditions.MovieAttribute> movie_condition
	){
		deleteCharacterListInMovieActor(null, movie_condition);
	}
	
	public void deleteCharacterListInMovieActorByMovie(
		pojo.Movie movie 
	){
		//TODO get id in condition
		return;	
	}
	
	
}
