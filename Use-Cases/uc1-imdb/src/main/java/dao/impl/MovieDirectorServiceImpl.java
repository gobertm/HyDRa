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
import conditions.MovieDirectorAttribute;
import conditions.Operator;
import pojo.MovieDirector;
import tdo.MovieTDO;
import tdo.MovieDirectorTDO;
import pojo.Movie;
import conditions.MovieAttribute;
import tdo.DirectorTDO;
import tdo.MovieDirectorTDO;
import pojo.Director;
import conditions.DirectorAttribute;
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


public class MovieDirectorServiceImpl extends dao.services.MovieDirectorService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MovieDirectorServiceImpl.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	//join structure
	// Left side 'movie_id' of reference [has_directed ]
	public Dataset<MovieTDO> getMovieTDOListDirected_movieInHas_directedInMovieKVFromMovieRedis(Condition<MovieAttribute> condition, MutableBoolean refilterFlag){	
		// Build the key pattern
		//  - If the condition attribute is in the key pattern, replace by the value. Only if operator is EQUALS.
		//  - Replace all other fields of key pattern by a '*' 
		String keypattern= "", keypatternAllVariables="";
		String valueCond=null;
		String finalKeypattern;
		List<String> fieldsListInKey = new ArrayList<>();
		Set<MovieAttribute> keyAttributes = new HashSet<>();
		keypattern=keypattern.concat("movie:");
		keypatternAllVariables=keypatternAllVariables.concat("movie:");
		if(!Util.containsOrCondition(condition)){
			valueCond=Util.getStringValue(Util.getValueOfAttributeInEqualCondition(condition,MovieAttribute.id));
			keyAttributes.add(MovieAttribute.id);
		}
		else{
			valueCond=null;
			refilterFlag.setValue(true);
		}
		if(valueCond==null)
			keypattern=keypattern.concat("*");
		else
			keypattern=keypattern.concat(valueCond);
		fieldsListInKey.add("id");
		keypatternAllVariables=keypatternAllVariables.concat("*");
		if(!refilterFlag.booleanValue()){
			Set<MovieAttribute> conditionAttributes = Util.getConditionAttributes(condition);
			for (MovieAttribute a : conditionAttributes) {
				if (!keyAttributes.contains(a)) {
					refilterFlag.setValue(true);
					break;
				}
			}
		}
			
		// Find the type of query to perform in order to retrieve a Dataset<Row>
		// Based on the type of the value. Is a it a simple string or a hash or a list... 
		Dataset<Row> rows;
			// TODO only handles 'hash' for now
		StructType structTypeHash = new StructType(new StructField[] {
			DataTypes.createStructField("id", DataTypes.StringType, true)
		,
			DataTypes.createStructField("title", DataTypes.StringType, true)
	,		DataTypes.createStructField("originalTitle", DataTypes.StringType, true)
	,		DataTypes.createStructField("isAdult", DataTypes.StringType, true)
	,		DataTypes.createStructField("startYear", DataTypes.StringType, true)
	,		DataTypes.createStructField("runtimeMinutes", DataTypes.StringType, true)
		});
		rows = SparkConnectionMgr.getRowsFromKeyValueHashes("myredis",keypattern, structTypeHash);
		boolean isStriped = (StringUtils.countMatches(keypattern, "*") == 1 && keypattern.endsWith("*"));
		String prefix=isStriped?keypattern.substring(0, keypattern.length() - 1):"";
		finalKeypattern = keypatternAllVariables;
		Dataset<MovieTDO> res = rows.map((MapFunction<Row, MovieTDO>) r -> {
					MovieTDO movie_res = new MovieTDO();
					Integer groupindex = null;
					String regex = null;
					String value = null;
					Pattern p, pattern = null;
					Matcher m, match = null;
					boolean matches = false;
					String key = isStriped ? prefix + r.getAs("id") : r.getAs("id");
					// Spark Redis automatically strips leading character if the pattern provided contains a single '*' at the end.				
					pattern = Pattern.compile("\\*");
			        match = pattern.matcher(finalKeypattern);
					regex = finalKeypattern.replaceAll("\\*","(.*)");
					p = Pattern.compile(regex);
					m = p.matcher(key);
					matches = m.find();
					// attribute [Movie.Id]
					// Attribute mapped in a key.
					groupindex = fieldsListInKey.indexOf("id")+1;
					if(groupindex==null) {
						logger.warn("Attribute 'Movie' mapped physical field 'id' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
					}
					String id = null;
					if(matches) {
						id = m.group(groupindex.intValue());
					} else {
						logger.warn("Cannot retrieve value for Movieid attribute stored in db myredis. Probably due to an ambiguous regex.");
						movie_res.addLogEvent("Cannot retrieve value for Movie.id attribute stored in db myredis. Probably due to an ambiguous regex.");
					}
					movie_res.setId(id == null ? null : id);
					// attribute [Movie.PrimaryTitle]
					String primaryTitle = r.getAs("title") == null ? null : r.getAs("title");
					movie_res.setPrimaryTitle(primaryTitle);
					// attribute [Movie.OriginalTitle]
					String originalTitle = r.getAs("originalTitle") == null ? null : r.getAs("originalTitle");
					movie_res.setOriginalTitle(originalTitle);
					// attribute [Movie.IsAdult]
					Boolean isAdult = r.getAs("isAdult") == null ? null : Boolean.parseBoolean(r.getAs("isAdult"));
					movie_res.setIsAdult(isAdult);
					// attribute [Movie.StartYear]
					Integer startYear = r.getAs("startYear") == null ? null : Integer.parseInt(r.getAs("startYear"));
					movie_res.setStartYear(startYear);
					// attribute [Movie.RuntimeMinutes]
					Integer runtimeMinutes = r.getAs("runtimeMinutes") == null ? null : Integer.parseInt(r.getAs("runtimeMinutes"));
					movie_res.setRuntimeMinutes(runtimeMinutes);
					//Checking that reference field 'id' is mapped in Key
					if(fieldsListInKey.contains("id")){
						//Retrieving reference field 'id' in Key
						Pattern pattern_id = Pattern.compile("\\*");
				        Matcher match_id = pattern_id.matcher(finalKeypattern);
						groupindex = fieldsListInKey.indexOf("id")+1;
						if(groupindex==null) {
							logger.warn("Attribute 'Movie' mapped physical field 'id' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
						}
						p = Pattern.compile(regex);
						m = p.matcher(key);
						matches = m.find();
						String has_directed_id = null;
						if(matches) {
						has_directed_id = m.group(groupindex.intValue());
						} else {
						logger.warn("Cannot retrieve value 'id'. Probably due to an ambiguous regex.");
						movie_res.addLogEvent("Cannot retrieve value for 'id' attribute stored in db myredis. Probably due to an ambiguous regex.");
						}
						movie_res.setMyRelSchema_directed_has_directed_id(has_directed_id);
					}else{
					// Get reference column in value [id ] for reference [has_directed]
					String myRelSchema_directed_has_directed_id = r.getAs("id");
					movie_res.setMyRelSchema_directed_has_directed_id(myRelSchema_directed_has_directed_id);
					}
	
						return movie_res;
				}, Encoders.bean(MovieTDO.class));
		if(refilterFlag.booleanValue())
			res = res.filter((FilterFunction<MovieTDO>) r -> condition == null || condition.evaluate(r));
		res=res.dropDuplicates();
		return res;
	}
	
	
	
	//join structure
	// Left side 'movie_id' of reference [movie_info ]
	public Dataset<MovieTDO> getMovieTDOListDirected_movieInMovie_infoInActorCollectionFromIMDB_Mongo(Condition<MovieAttribute> condition, MutableBoolean refilterFlag){	
		String bsonQuery = MovieServiceImpl.getBSONMatchQueryInActorCollectionFromMymongo(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mymongo", "actorCollection", bsonQuery);
	
		Dataset<MovieTDO> res = dataset.flatMap((FlatMapFunction<Row, MovieTDO>) r -> {
				List<MovieTDO> list_res = new ArrayList<MovieTDO>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				MovieTDO movie1 = new MovieTDO();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					
						// field  id for reference movie_info
					nestedRow =  r1;
					if(nestedRow != null) {
						movie1.setMyRelSchema_directed_movie_info_id(nestedRow.getAs("id") == null ? null : nestedRow.getAs("id").toString());
						toAdd1 = true;					
					}
					
					
					array1 = r1.getAs("movies");
					if(array1!= null) {
						for (int i2 = 0; i2 < array1.size(); i2++){
							Row r2 = (Row) array1.apply(i2);
							MovieTDO movie2 = (MovieTDO) movie1.clone();
							boolean toAdd2  = false;
							WrappedArray array2  = null;
							// 	attribute Movie.id for field id			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("id")) {
								if(nestedRow.getAs("id") == null){
									movie2.setId(null);
								}else{
									movie2.setId((String) nestedRow.getAs("id"));
									toAdd2 = true;					
									}
							}
							// 	attribute Movie.primaryTitle for field title			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("title")) {
								if(nestedRow.getAs("title") == null){
									movie2.setPrimaryTitle(null);
								}else{
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
									movie2.setAverageRating(averageRating == null ? null : averageRating);
									toAdd2 = true;
								} else {
									throw new Exception("Cannot retrieve value for Movie.averageRating attribute stored in db mymongo. Probably due to an ambiguous regex.");
								}
							}
							// 	attribute Movie.numVotes for field numberofvotes			
							nestedRow =  r2;
							nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("rating");
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("numberofvotes")) {
								if(nestedRow.getAs("numberofvotes") == null){
									movie2.setNumVotes(null);
								}else{
									movie2.setNumVotes((Integer) nestedRow.getAs("numberofvotes"));
									toAdd2 = true;					
									}
							}
							
								// field  id for reference movie_info
							nestedRow =  r2;
							if(nestedRow != null) {
								movie2.setMyRelSchema_directed_movie_info_id(nestedRow.getAs("id") == null ? null : nestedRow.getAs("id").toString());
								toAdd2 = true;					
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
	
		}, Encoders.bean(MovieTDO.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
	}
	
	
	
	public Dataset<MovieDirectorTDO> getMovieDirectorTDOListIndirectorTableAnddirectedFrommydb(Condition<DirectorAttribute> director_cond, MutableBoolean refilterFlag) {
		Pair<String, List<String>> whereClause = DirectorServiceImpl.getSQLWhereClauseInDirectorTableFromMydbWithTableAlias(director_cond, refilterFlag, "directorTable.");
		
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", "'" + Util.escapeQuote(preparedValue) + "'");
		}
	
		where = (where == null) ? "" : (" AND " + where);
		String aliasedColumns = "directorTable.id as directorTable_id,directorTable.fullname as directorTable_fullname,directorTable.birth as directorTable_birth,directorTable.death as directorTable_death, directed.director_id as directed_director_id,directed.movie_id as directed_movie_id";
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("mydb", "(SELECT " + aliasedColumns + " FROM directorTable, directed WHERE directed.director_id = directorTable.id" + where + ") AS JOIN_TABLE");
		Dataset<MovieDirectorTDO> res = d.map((MapFunction<Row, MovieDirectorTDO>) r -> {
					MovieDirectorTDO movieDirector_res = new MovieDirectorTDO();
					movieDirector_res.setDirector(new Director());
					
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Director.Id]
					String director_id = Util.getStringValue(r.getAs("directorTable_id"));
					movieDirector_res.getDirector().setId(director_id);
					
					// attribute [Director.FirstName]
					regex = "(.*)( )(.*)";
					groupIndex = 1;
					if(groupIndex == null) {
						logger.warn("Cannot retrieve value for Director.firstName attribute stored in db mydb. Probably due to an ambiguous regex.");
						throw new Exception("Cannot retrieve value for Director.firstName attribute stored in db mydb. Probably due to an ambiguous regex.");
					}
					value = r.getAs("directorTable_fullname");
					p = Pattern.compile(regex);
					m = p.matcher(value);
					matches = m.find();
					String director_firstName = null;
					if(matches) {
						director_firstName = m.group(groupIndex.intValue());
					} else {
						logger.warn("Cannot retrieve value for Director.firstName attribute stored in db mydb. Probably due to an ambiguous regex.");
						throw new Exception("Cannot retrieve value for Director.firstName attribute stored in db mydb. Probably due to an ambiguous regex.");
					}
					movieDirector_res.getDirector().setFirstName(director_firstName == null ? null : director_firstName);
					
					// attribute [Director.LastName]
					regex = "(.*)( )(.*)";
					groupIndex = 3;
					if(groupIndex == null) {
						logger.warn("Cannot retrieve value for Director.lastName attribute stored in db mydb. Probably due to an ambiguous regex.");
						throw new Exception("Cannot retrieve value for Director.lastName attribute stored in db mydb. Probably due to an ambiguous regex.");
					}
					value = r.getAs("directorTable_fullname");
					p = Pattern.compile(regex);
					m = p.matcher(value);
					matches = m.find();
					String director_lastName = null;
					if(matches) {
						director_lastName = m.group(groupIndex.intValue());
					} else {
						logger.warn("Cannot retrieve value for Director.lastName attribute stored in db mydb. Probably due to an ambiguous regex.");
						throw new Exception("Cannot retrieve value for Director.lastName attribute stored in db mydb. Probably due to an ambiguous regex.");
					}
					movieDirector_res.getDirector().setLastName(director_lastName == null ? null : director_lastName);
					
					// attribute [Director.YearOfBirth]
					Integer director_yearOfBirth = Util.getIntegerValue(r.getAs("directorTable_birth"));
					movieDirector_res.getDirector().setYearOfBirth(director_yearOfBirth);
					
					// attribute [Director.YearOfDeath]
					Integer director_yearOfDeath = Util.getIntegerValue(r.getAs("directorTable_death"));
					movieDirector_res.getDirector().setYearOfDeath(director_yearOfDeath);
					
					String has_directed_movie_id = r.getAs("directed_movie_id") == null ? null : r.getAs("directed_movie_id").toString();
					movieDirector_res.setMyRelSchema_directed_has_directed_movie_id(has_directed_movie_id);
					String movie_info_movie_id = r.getAs("directed_movie_id") == null ? null : r.getAs("directed_movie_id").toString();
					movieDirector_res.setMyRelSchema_directed_movie_info_movie_id(movie_info_movie_id);
					
		
					return movieDirector_res;
				}, Encoders.bean(MovieDirectorTDO.class));
		
		return res;
	}
	
	
	
	
	
	
	
	
	public java.util.List<pojo.MovieDirector> getMovieDirectorList(
		conditions.Condition<conditions.MovieAttribute> directed_movie_condition,
		conditions.Condition<conditions.DirectorAttribute> director_condition){
			//TODO
			return null;
		}
	
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
	
	public void insertMovieDirector(MovieDirector movieDirector){
		//TODO
	}
	
	public void deleteMovieDirectorList(
		conditions.Condition<conditions.MovieAttribute> directed_movie_condition,
		conditions.Condition<conditions.DirectorAttribute> director_condition){
			//TODO
		}
	
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
