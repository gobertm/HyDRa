package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.User;
import conditions.*;
import dao.services.UserService;
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


public class UserServiceImpl extends UserService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserServiceImpl.class);
	
	
	
	
	public static String getBSONMatchQueryInReviewColFromMymongo(Condition<UserAttribute> condition, MutableBoolean refilterFlag) {	
		String res = null;	
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				UserAttribute attr = ((SimpleCondition<UserAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<UserAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<UserAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == UserAttribute.id ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "authorid': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "author." + res;
					res = "'" + res;
					}
					if(attr == UserAttribute.username ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "username': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "author." + res;
					res = "'" + res;
					}
					if(attr == UserAttribute.city ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "city': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "author." + res;
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
	
	public Dataset<User> getUserListInReviewColFromMymongo(conditions.Condition<conditions.UserAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = UserServiceImpl.getBSONMatchQueryInReviewColFromMymongo(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mymongo", "reviewCol", bsonQuery);
	
		Dataset<User> res = dataset.flatMap((FlatMapFunction<Row, User>) r -> {
				List<User> list_res = new ArrayList<User>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				User user1 = new User();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute User.id for field authorid			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("author");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("authorid")) {
						if(nestedRow.getAs("authorid")==null)
							user1.setId(null);
						else{
							user1.setId((String) nestedRow.getAs("authorid"));
							toAdd1 = true;					
							}
					}
					// 	attribute User.username for field username			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("author");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("username")) {
						if(nestedRow.getAs("username")==null)
							user1.setUsername(null);
						else{
							user1.setUsername((String) nestedRow.getAs("username"));
							toAdd1 = true;					
							}
					}
					// 	attribute User.city for field city			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("author");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("city")) {
						if(nestedRow.getAs("city")==null)
							user1.setCity(null);
						else{
							user1.setCity((String) nestedRow.getAs("city"));
							toAdd1 = true;					
							}
					}
					if(toAdd1) {
						
						list_res.add(user1);
						addedInList = true;
					} 
					
				
				return list_res.iterator();
	
		}, Encoders.bean(User.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
		
	}
	
	
	// TODO get based on id(s). Ex:public Client getClientById(Long id)
	
	public Dataset<User> getUserListById(String id) {
		return getUserList(conditions.Condition.simple(conditions.UserAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<User> getUserListByUsername(String username) {
		return getUserList(conditions.Condition.simple(conditions.UserAttribute.username, conditions.Operator.EQUALS, username));
	}
	
	public Dataset<User> getUserListByCity(String city) {
		return getUserList(conditions.Condition.simple(conditions.UserAttribute.city, conditions.Operator.EQUALS, city));
	}
	
	
	
	
	public Dataset<User> getR_authorListInReviewUser(conditions.Condition<conditions.UserAttribute> r_author_condition,conditions.Condition<conditions.ReviewAttribute> r_review1_condition)		{
		MutableBoolean r_author_refilter = new MutableBoolean(false);
		List<Dataset<User>> datasetsPOJO = new ArrayList<Dataset<User>>();
		Dataset<Review> all = new ReviewServiceImpl().getReviewList(r_review1_condition);
		boolean all_already_persisted = false;
		MutableBoolean r_review1_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		
		Dataset<ReviewUser> res_reviewUser_r_author;
		Dataset<User> res_User;
		// Role 'r_review1' mapped to EmbeddedObject 'author' 'User' containing 'Review' 
		r_review1_refilter = new MutableBoolean(false);
		res_reviewUser_r_author = reviewUserService.getReviewUserListInIMDB_MongoreviewColauthor(r_review1_condition, r_author_condition, r_review1_refilter, r_author_refilter);
		if(r_review1_refilter.booleanValue()) {
			joinCondition = null;
			joinCondition = res_reviewUser_r_author.col("r_review1.id").equalTo(all.col("id"));
			if(joinCondition == null)
				res_User = res_reviewUser_r_author.join(all).select("r_author.*").as(Encoders.bean(User.class));
			else
				res_User = res_reviewUser_r_author.join(all, joinCondition).select("r_author.*").as(Encoders.bean(User.class));
		
		} else
			res_User = res_reviewUser_r_author.map((MapFunction<ReviewUser,User>) r -> r.getR_author(), Encoders.bean(User.class));
		res_User = res_User.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_User);
		
		
		//Join datasets or return 
		Dataset<User> res = fullOuterJoinsUser(datasetsPOJO);
		if(res == null)
			return null;
	
		if(r_author_refilter.booleanValue())
			res = res.filter((FilterFunction<User>) r -> r_author_condition == null || r_author_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<User> getR_authorListInReviewUserByR_authorCondition(conditions.Condition<conditions.UserAttribute> r_author_condition){
		return getR_authorListInReviewUser(r_author_condition, null);
	}
	public Dataset<User> getR_authorListInReviewUserByR_review1Condition(conditions.Condition<conditions.ReviewAttribute> r_review1_condition){
		return getR_authorListInReviewUser(null, r_review1_condition);
	}
	
	public User getR_authorInReviewUserByR_review1(pojo.Review r_review1){
		if(r_review1 == null)
			return null;
	
		Condition c;
		c=Condition.simple(ReviewAttribute.id,Operator.EQUALS, r_review1.getId());
		Dataset<User> res = getR_authorListInReviewUserByR_review1Condition(c);
		return !res.isEmpty()?res.first():null;
	}
	
	
	public boolean insertUser(User user){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
		return inserted;
	}
	
	
	
	public void updateUserList(conditions.Condition<conditions.UserAttribute> condition, conditions.SetClause<conditions.UserAttribute> set){
		//TODO
	}
	
	public void updateUser(pojo.User user) {
		//TODO using the id
		return;
	}
	public void updateR_authorListInReviewUser(
		conditions.Condition<conditions.UserAttribute> r_author_condition,
		conditions.Condition<conditions.ReviewAttribute> r_review1_condition,
		
		conditions.SetClause<conditions.UserAttribute> set
	){
		//TODO
	}
	
	public void updateR_authorListInReviewUserByR_authorCondition(
		conditions.Condition<conditions.UserAttribute> r_author_condition,
		conditions.SetClause<conditions.UserAttribute> set
	){
		updateR_authorListInReviewUser(r_author_condition, null, set);
	}
	public void updateR_authorListInReviewUserByR_review1Condition(
		conditions.Condition<conditions.ReviewAttribute> r_review1_condition,
		conditions.SetClause<conditions.UserAttribute> set
	){
		updateR_authorListInReviewUser(null, r_review1_condition, set);
	}
	
	public void updateR_authorInReviewUserByR_review1(
		pojo.Review r_review1,
		conditions.SetClause<conditions.UserAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	
	
	public void deleteUserList(conditions.Condition<conditions.UserAttribute> condition){
		//TODO
	}
	
	public void deleteUser(pojo.User user) {
		//TODO using the id
		return;
	}
	public void deleteR_authorListInReviewUser(	
		conditions.Condition<conditions.UserAttribute> r_author_condition,	
		conditions.Condition<conditions.ReviewAttribute> r_review1_condition){
			//TODO
		}
	
	public void deleteR_authorListInReviewUserByR_authorCondition(
		conditions.Condition<conditions.UserAttribute> r_author_condition
	){
		deleteR_authorListInReviewUser(r_author_condition, null);
	}
	public void deleteR_authorListInReviewUserByR_review1Condition(
		conditions.Condition<conditions.ReviewAttribute> r_review1_condition
	){
		deleteR_authorListInReviewUser(null, r_review1_condition);
	}
	
	public void deleteR_authorInReviewUserByR_review1(
		pojo.Review r_review1 
	){
		//TODO get id in condition
		return;	
	}
	
	
}
