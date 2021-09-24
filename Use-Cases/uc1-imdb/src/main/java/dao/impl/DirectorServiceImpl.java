package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Director;
import conditions.*;
import dao.services.DirectorService;
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
					String id = Util.getStringValue(r.getAs("id"));
					director_res.setId(id);
					
					// attribute [Director.FirstName]
					regex = "(.*)( )(.*)";
					groupIndex = 1;
					if(groupIndex == null) {
						logger.warn("Cannot retrieve value for DirectorfirstName attribute stored in db mydb. Probably due to an ambiguous regex.");
						director_res.addLogEvent("Cannot retrieve value for DirectorfirstName attribute stored in db mydb. Probably due to an ambiguous regex.");
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
						director_res.addLogEvent("Cannot retrieve value for Director.firstName attribute stored in db mydb. Probably due to an ambiguous regex.");
					}
					director_res.setFirstName(firstName == null ? null : firstName);
					
					// attribute [Director.LastName]
					regex = "(.*)( )(.*)";
					groupIndex = 3;
					if(groupIndex == null) {
						logger.warn("Cannot retrieve value for DirectorlastName attribute stored in db mydb. Probably due to an ambiguous regex.");
						director_res.addLogEvent("Cannot retrieve value for DirectorlastName attribute stored in db mydb. Probably due to an ambiguous regex.");
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
						director_res.addLogEvent("Cannot retrieve value for Director.lastName attribute stored in db mydb. Probably due to an ambiguous regex.");
					}
					director_res.setLastName(lastName == null ? null : lastName);
					
					// attribute [Director.YearOfBirth]
					Integer yearOfBirth = Util.getIntegerValue(r.getAs("birth"));
					director_res.setYearOfBirth(yearOfBirth);
					
					// attribute [Director.YearOfDeath]
					Integer yearOfDeath = Util.getIntegerValue(r.getAs("death"));
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
		// (A - AB) (B)
		directed_movie_refilter = new MutableBoolean(false);
		Dataset<MovieDirectorTDO> res_movieDirector_directed_by_has_directed = movieDirectorService.getMovieDirectorTDOListIndirectorTableAnddirectedFrommydb(director_condition, director_refilter);
		Dataset<MovieTDO> res_has_directed_directed_by = movieDirectorService.getMovieTDOListDirected_movieInHas_directedInMovieKVFromMovieRedis(directed_movie_condition, directed_movie_refilter);
		if(directed_movie_refilter.booleanValue()) {
				joinCondition = null;
				joinCondition = res_has_directed_directed_by.col("id").equalTo(all.col("id"));
				res_has_directed_directed_by = res_has_directed_directed_by.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(MovieTDO.class));
		}
		
		Dataset<Row> res_row_directed_by_has_directed = res_movieDirector_directed_by_has_directed.join(res_has_directed_directed_by.withColumnRenamed("logEvents", "movieDirector_logEvents"),
															res_has_directed_directed_by.col("myRelSchema_directed_has_directed_id").equalTo(res_movieDirector_directed_by_has_directed.col("myRelSchema_directed_has_directed_movie_id")));
		Dataset<Director> res_Director_directed_by = res_row_directed_by_has_directed.select("director.*").as(Encoders.bean(Director.class));
		datasetsPOJO.add(res_Director_directed_by.dropDuplicates(new String[] {"id"}));	
		
		
		
		Dataset<MovieDirector> res_movieDirector_director;
		Dataset<Director> res_Director;
		
		
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
	
	public boolean insertDirector(Director director){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertDirectorInDirectorTableFromMydb(director) || inserted ;
		return inserted;
	}
	
	public boolean insertDirectorInDirectorTableFromMydb(Director director)	{
		Condition<DirectorAttribute> conditionID;
		String idvalue="";
		conditionID = Condition.simple(DirectorAttribute.id, Operator.EQUALS, director.getId());
		idvalue+=director.getId();
		boolean entityExists=false;
		entityExists = !getDirectorListInDirectorTableFromMydb(conditionID,new MutableBoolean(false)).isEmpty();
				
		if(!entityExists){
		List<Row> listRows=new ArrayList<Row>();
		List<Object> valuesdirectorTable_1 = new ArrayList<>();
		List<StructField> listOfStructFielddirectorTable_1 = new ArrayList<StructField>();
		if(!listOfStructFielddirectorTable_1.contains(DataTypes.createStructField("id",DataTypes.StringType, true)))
			listOfStructFielddirectorTable_1.add(DataTypes.createStructField("id",DataTypes.StringType, true));
		valuesdirectorTable_1.add(director.getId());
		String value_directorTable_fullname_1 = "";
		value_directorTable_fullname_1 += director.getFirstName();
		value_directorTable_fullname_1 += " ";
		value_directorTable_fullname_1 += director.getLastName();
		if(!listOfStructFielddirectorTable_1.contains(DataTypes.createStructField("fullname",DataTypes.StringType, true)))
			listOfStructFielddirectorTable_1.add(DataTypes.createStructField("fullname",DataTypes.StringType, true));
		valuesdirectorTable_1.add(value_directorTable_fullname_1);
		if(!listOfStructFielddirectorTable_1.contains(DataTypes.createStructField("birth",DataTypes.IntegerType, true)))
			listOfStructFielddirectorTable_1.add(DataTypes.createStructField("birth",DataTypes.IntegerType, true));
		valuesdirectorTable_1.add(director.getYearOfBirth());
		if(!listOfStructFielddirectorTable_1.contains(DataTypes.createStructField("death",DataTypes.IntegerType, true)))
			listOfStructFielddirectorTable_1.add(DataTypes.createStructField("death",DataTypes.IntegerType, true));
		valuesdirectorTable_1.add(director.getYearOfDeath());
		
		StructType structType = DataTypes.createStructType(listOfStructFielddirectorTable_1);
		listRows.add(RowFactory.create(valuesdirectorTable_1.toArray()));
		SparkConnectionMgr.writeDataset(listRows, structType, "jdbc", "directorTable", "mydb");
			logger.info("Inserted [Director] entity ID [{}] in [DirectorTable] in database [Mydb]", idvalue);
		}
		else
			logger.warn("[Director] entity ID [{}] already present in [DirectorTable] in database [Mydb]", idvalue);
		return !entityExists;
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
