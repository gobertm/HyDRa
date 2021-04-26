package dao.impl;

import java.util.Arrays;
import java.util.List;
import pojo.Director;
import conditions.*;
import dao.services.DirectorService;
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

public class DirectorServiceImpl extends DirectorService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DirectorServiceImpl.class);
	
	
	public static Pair<String, List<String>> getSQLWhereClauseInDirectorTableFromMydb(Condition<DirectorAttribute> condition, MutableBoolean refilterFlag) {
		return getSQLWhereClauseInDirectorTableFromMydbWithTableAlias(condition, refilterFlag, "");
	}
	
	public static Pair<String, List<String>> getSQLWhereClauseInDirectorTableFromMydbWithTableAlias(Condition<DirectorAttribute> condition, MutableBoolean refilterFlag, String tableAlias) {
		String where = null;	
		List<String> preparedValues = new java.util.ArrayList<String>();
		if(condition != null) {
			
			if(condition instanceof SimpleCondition) {
				DirectorAttribute attr = ((SimpleCondition<DirectorAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<DirectorAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<DirectorAttribute>) condition).getValue();
				if(value != null) {
					boolean isConditionAttrEncountered = false;
					if(attr == DirectorAttribute.id ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "id " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(attr == DirectorAttribute.firstName ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = "@VAR@ @OTHERVAR@";
						Boolean like_op = false;
						if(op == Operator.EQUALS && valueString != null && preparedValue.contains("@OTHERVAR@")) {
							//ex: @@VAR@@' '@@OTHERVAR@@"=> more than one vars in LongField: we shall use regex
							like_op = true;
							sqlOp = "LIKE";
							preparedValue = "@VAR@ @OTHERVAR@";
						} else {
							if(op == Operator.NOT_EQUALS && valueString != null && preparedValue.contains("@OTHERVAR@")) {
								//ex: @@VAR@@' '@@OTHERVAR@@"=> more than one vars in LongField: we shall use regex
								sqlOp = "NOT LIKE";
								like_op = true;
								preparedValue = "@VAR@ @OTHERVAR@";
							}
						}
						if(op == Operator.CONTAINS && valueString != null) {
							like_op = true;
							preparedValue = "@VAR@ @OTHERVAR@";
							preparedValue = preparedValue.replaceAll("@VAR@", "%@VAR@%");
						}
						
						if(like_op)
							valueString = Util.escapeReservedCharSQL(valueString);
						preparedValue = preparedValue.replaceAll("@VAR@", valueString).replaceAll("@OTHERVAR@", "%");
						
						where = tableAlias + "fullname " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(attr == DirectorAttribute.lastName ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = "@OTHERVAR@ @VAR@";
						Boolean like_op = false;
						if(op == Operator.EQUALS && valueString != null && preparedValue.contains("@OTHERVAR@")) {
							//ex: @@VAR@@' '@@OTHERVAR@@"=> more than one vars in LongField: we shall use regex
							like_op = true;
							sqlOp = "LIKE";
							preparedValue = "@OTHERVAR@ @VAR@";
						} else {
							if(op == Operator.NOT_EQUALS && valueString != null && preparedValue.contains("@OTHERVAR@")) {
								//ex: @@VAR@@' '@@OTHERVAR@@"=> more than one vars in LongField: we shall use regex
								sqlOp = "NOT LIKE";
								like_op = true;
								preparedValue = "@OTHERVAR@ @VAR@";
							}
						}
						if(op == Operator.CONTAINS && valueString != null) {
							like_op = true;
							preparedValue = "@OTHERVAR@ @VAR@";
							preparedValue = preparedValue.replaceAll("@VAR@", "%@VAR@%");
						}
						
						if(like_op)
							valueString = Util.escapeReservedCharSQL(valueString);
						preparedValue = preparedValue.replaceAll("@VAR@", valueString).replaceAll("@OTHERVAR@", "%");
						
						where = tableAlias + "fullname " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(attr == DirectorAttribute.yearOfBirth ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "birth " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(attr == DirectorAttribute.yearOfDeath ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "death " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						where = "1 = 1";
					}
				}
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInDirectorTableFromMydb(((AndCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInDirectorTableFromMydb(((AndCondition) condition).getRightCondition(), refilterFlag);
				String whereLeft = pairLeft.getKey();
				String whereRight = pairRight.getKey();
				List<String> leftValues = pairLeft.getValue();
				List<String> rightValues = pairRight.getValue();
				if(whereLeft != null || whereRight != null) {
					if(whereLeft == null)
						where = whereRight;
					else
						if(whereRight == null)
							where = whereLeft;
						else
							where = "(" + whereLeft + " AND " + whereRight + ")";
					preparedValues.addAll(leftValues);
					preparedValues.addAll(rightValues);
				}
			}
	
			if(condition instanceof OrCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInDirectorTableFromMydb(((OrCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInDirectorTableFromMydb(((OrCondition) condition).getRightCondition(), refilterFlag);
				String whereLeft = pairLeft.getKey();
				String whereRight = pairRight.getKey();
				List<String> leftValues = pairLeft.getValue();
				List<String> rightValues = pairRight.getValue();
				if(whereLeft != null || whereRight != null) {
					if(whereLeft == null)
						where = whereRight;
					else
						if(whereRight == null)
							where = whereLeft;
						else
							where = "(" + whereLeft + " OR " + whereRight + ")";
					preparedValues.addAll(leftValues);
					preparedValues.addAll(rightValues);
				}
			}
	
		}
	
		return new ImmutablePair<String, List<String>>(where, preparedValues);
	}
	
	
	public Dataset<Director> getDirectorListInDirectorTableFromMydb(conditions.Condition<conditions.DirectorAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = DirectorServiceImpl.getSQLWhereClauseInDirectorTableFromMydb(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", "'" + Util.escapeQuote(preparedValue) + "'");
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("mydb", "directorTable");
		if(where != null) {
			d = d.where(where);
		}
	
		Dataset<Director> res = d.map((MapFunction<Row, Director>) r -> {
					Director director_res = new Director();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Director.Id]
					String id = r.getAs("id");
					director_res.setId(id);
					
					// attribute [Director.FirstName]
					regex = "(.*)( )(.*)";
					groupIndex = 1;
					if(groupIndex == null) {
						logger.warn("Cannot retrieve value for DirectorfirstName attribute stored in db mydb. Probably due to an ambiguous regex.");
						throw new Exception("Cannot retrieve value for DirectorfirstName attribute stored in db mydb. Probably due to an ambiguous regex.");
					}
					value = r.getAs("fullname");
					p = Pattern.compile(regex);
					m = p.matcher(value);
					matches = m.find();
					String firstName = null;
					if(matches) {
						firstName = m.group(groupIndex.intValue());
					} else {
						logger.warn("Cannot retrieve value for DirectorfirstName attribute stored in db mydb. Probably due to an ambiguous regex.");
						throw new Exception("Cannot retrieve value for DirectorfirstName attribute stored in db mydb. Probably due to an ambiguous regex.");
					}
					director_res.setFirstName(firstName);
					
					// attribute [Director.LastName]
					regex = "(.*)( )(.*)";
					groupIndex = 3;
					if(groupIndex == null) {
						logger.warn("Cannot retrieve value for DirectorlastName attribute stored in db mydb. Probably due to an ambiguous regex.");
						throw new Exception("Cannot retrieve value for DirectorlastName attribute stored in db mydb. Probably due to an ambiguous regex.");
					}
					value = r.getAs("fullname");
					p = Pattern.compile(regex);
					m = p.matcher(value);
					matches = m.find();
					String lastName = null;
					if(matches) {
						lastName = m.group(groupIndex.intValue());
					} else {
						logger.warn("Cannot retrieve value for DirectorlastName attribute stored in db mydb. Probably due to an ambiguous regex.");
						throw new Exception("Cannot retrieve value for DirectorlastName attribute stored in db mydb. Probably due to an ambiguous regex.");
					}
					director_res.setLastName(lastName);
					
					// attribute [Director.YearOfBirth]
					Integer yearOfBirth = r.getAs("birth");
					director_res.setYearOfBirth(yearOfBirth);
					
					// attribute [Director.YearOfDeath]
					Integer yearOfDeath = r.getAs("death");
					director_res.setYearOfDeath(yearOfDeath);
	
	
	
					return director_res;
				}, Encoders.bean(Director.class));
	
	
		return res;
		
	}
	
	
	// TODO get based on id(s). Ex:public Client getClientById(Long id)
	
	public Dataset<Director> getDirectorListById(String id) {
		return getDirectorList(conditions.Condition.simple(conditions.DirectorAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Director> getDirectorListByFirstName(String firstName) {
		return getDirectorList(conditions.Condition.simple(conditions.DirectorAttribute.firstName, conditions.Operator.EQUALS, firstName));
	}
	
	public Dataset<Director> getDirectorListByLastName(String lastName) {
		return getDirectorList(conditions.Condition.simple(conditions.DirectorAttribute.lastName, conditions.Operator.EQUALS, lastName));
	}
	
	public Dataset<Director> getDirectorListByYearOfBirth(Integer yearOfBirth) {
		return getDirectorList(conditions.Condition.simple(conditions.DirectorAttribute.yearOfBirth, conditions.Operator.EQUALS, yearOfBirth));
	}
	
	public Dataset<Director> getDirectorListByYearOfDeath(Integer yearOfDeath) {
		return getDirectorList(conditions.Condition.simple(conditions.DirectorAttribute.yearOfDeath, conditions.Operator.EQUALS, yearOfDeath));
	}
	
	
	
	
	public Dataset<Director> getDirectorListInMovieDirector(conditions.Condition<conditions.MovieAttribute> directed_movie_condition,conditions.Condition<conditions.DirectorAttribute> director_condition)		{
		MutableBoolean director_refilter = new MutableBoolean(false);
		List<Dataset<Director>> datasetsPOJO = new ArrayList<Dataset<Director>>();
		Dataset<Movie> all = new MovieServiceImpl().getMovieList(directed_movie_condition);
		boolean all_already_persisted = false;
		MutableBoolean directed_movie_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// join physical structure
	
		//join between 2 SQL tables and a non-relational structure
		directed_movie_refilter = new MutableBoolean(false);
		Dataset<MovieDirectorTDO> res_movieDirector_directed_by_movie_info = movieDirectorService.getMovieDirectorTDOListIndirectorTableAnddirectedFrommydb(director_condition, director_refilter);
		Dataset<MovieTDO> res_movie_info_directed_by = movieDirectorService.getMovieTDOListDirected_movieInMovie_infoInActorCollectionFromIMDB_Mongo(directed_movie_condition, directed_movie_refilter);
		if(directed_movie_refilter.booleanValue()) {
				joinCondition = null;
				joinCondition = res_movie_info_directed_by.col("id").equalTo(all.col("id"));
				res_movie_info_directed_by = res_movie_info_directed_by.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(MovieTDO.class));
		}
		
		Dataset<Row> res_row_directed_by_movie_info = res_movieDirector_directed_by_movie_info.join(res_movie_info_directed_by.withColumnRenamed("logEvents", "movieDirector_logEvents"),
			res_movie_info_directed_by.col("myRelSchema_directed_movie_info_id").equalTo(res_movieDirector_directed_by_movie_info.col("myRelSchema_directed_movie_info_movie_id")));
		Dataset<Director> res_Director_directed_by = res_row_directed_by_movie_info.select("director.*").as(Encoders.bean(Director.class));
		datasetsPOJO.add(res_Director_directed_by.dropDuplicates(new String[] {"id"}));	
		
		
		
		
		
		//Join datasets or return 
		Dataset<Director> res = fullOuterJoinsDirector(datasetsPOJO);
		if(res == null)
			return null;
	
		if(director_refilter.booleanValue())
			res = res.filter((FilterFunction<Director>) r -> director_condition == null || director_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Director> getDirectorListInMovieDirectorByDirected_movieCondition(conditions.Condition<conditions.MovieAttribute> directed_movie_condition){
		return getDirectorListInMovieDirector(directed_movie_condition, null);
	}
	
	public Dataset<Director> getDirectorListInMovieDirectorByDirected_movie(pojo.Movie directed_movie){
		if(directed_movie == null)
			return null;
	
		Condition c;
		c=Condition.simple(MovieAttribute.id,Operator.EQUALS, directed_movie.getId());
		Dataset<Director> res = getDirectorListInMovieDirectorByDirected_movieCondition(c);
		return res;
	}
	
	public Dataset<Director> getDirectorListInMovieDirectorByDirectorCondition(conditions.Condition<conditions.DirectorAttribute> director_condition){
		return getDirectorListInMovieDirector(null, director_condition);
	}
	
	public void insertDirectorAndLinkedItems(Director director){
		//TODO
	}
	public void insertDirector(Director director){
		// Insert into all mapped AbstractPhysicalStructure 
			insertDirectorInDirectorTableFromMydb(director);
	}
	
	public void insertDirectorInDirectorTableFromMydb(Director director){
		//Read mapping rules and find attributes of the POJO that are mapped to the corresponding AbstractPhysicalStructure
		// Insert in SQL DB 
	String query = "INSERT INTO directorTable(fullname,death,fullname,birth,id) VALUES (?,?,?,?,?)";
	
	List<Object> inputs = new ArrayList<>();
	inputs.add(director.getLastName());
	inputs.add(director.getYearOfDeath());
	inputs.add(director.getFirstName());
	inputs.add(director.getYearOfBirth());
	inputs.add(director.getId());
	// Get the reference attribute. Either via a TDO Object or using the Pojo reference TODO
	DBConnectionMgr.getMapDB().get("mydb").insertOrUpdateOrDelete(query,inputs);
	}
	
	public void updateDirectorList(conditions.Condition<conditions.DirectorAttribute> condition, conditions.SetClause<conditions.DirectorAttribute> set){
		//TODO
	}
	
	public void updateDirector(pojo.Director director) {
		//TODO using the id
		return;
	}
	public void updateDirectorListInMovieDirector(
		conditions.Condition<conditions.MovieAttribute> directed_movie_condition,
		conditions.Condition<conditions.DirectorAttribute> director_condition,
		
		conditions.SetClause<conditions.DirectorAttribute> set
	){
		//TODO
	}
	
	public void updateDirectorListInMovieDirectorByDirected_movieCondition(
		conditions.Condition<conditions.MovieAttribute> directed_movie_condition,
		conditions.SetClause<conditions.DirectorAttribute> set
	){
		updateDirectorListInMovieDirector(directed_movie_condition, null, set);
	}
	
	public void updateDirectorListInMovieDirectorByDirected_movie(
		pojo.Movie directed_movie,
		conditions.SetClause<conditions.DirectorAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateDirectorListInMovieDirectorByDirectorCondition(
		conditions.Condition<conditions.DirectorAttribute> director_condition,
		conditions.SetClause<conditions.DirectorAttribute> set
	){
		updateDirectorListInMovieDirector(null, director_condition, set);
	}
	
	
	public void deleteDirectorList(conditions.Condition<conditions.DirectorAttribute> condition){
		//TODO
	}
	
	public void deleteDirector(pojo.Director director) {
		//TODO using the id
		return;
	}
	public void deleteDirectorListInMovieDirector(	
		conditions.Condition<conditions.MovieAttribute> directed_movie_condition,	
		conditions.Condition<conditions.DirectorAttribute> director_condition){
			//TODO
		}
	
	public void deleteDirectorListInMovieDirectorByDirected_movieCondition(
		conditions.Condition<conditions.MovieAttribute> directed_movie_condition
	){
		deleteDirectorListInMovieDirector(directed_movie_condition, null);
	}
	
	public void deleteDirectorListInMovieDirectorByDirected_movie(
		pojo.Movie directed_movie 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteDirectorListInMovieDirectorByDirectorCondition(
		conditions.Condition<conditions.DirectorAttribute> director_condition
	){
		deleteDirectorListInMovieDirector(null, director_condition);
	}
	
}
