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
import conditions.ReviewUserAttribute;
import conditions.Operator;
import pojo.ReviewUser;
import tdo.UserTDO;
import tdo.ReviewUserTDO;
import pojo.User;
import conditions.UserAttribute;
import tdo.ReviewTDO;
import tdo.ReviewUserTDO;
import pojo.Review;
import conditions.ReviewAttribute;
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


public class ReviewUserServiceImpl extends dao.services.ReviewUserService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReviewUserServiceImpl.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	
	
	// method accessing the embedded object author mapped to role r_review1
	public Dataset<pojo.ReviewUser> getReviewUserListInIMDB_MongoreviewColauthor(Condition<ReviewAttribute> r_review1_condition, Condition<UserAttribute> r_author_condition, MutableBoolean r_review1_refilter, MutableBoolean r_author_refilter){	
			List<String> bsons = new ArrayList<String>();
			String bson = null;
			bson = UserServiceImpl.getBSONMatchQueryInReviewColFromMymongo(r_author_condition ,r_author_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
			bson = ReviewServiceImpl.getBSONMatchQueryInReviewColFromMymongo(r_review1_condition ,r_review1_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
		
			String bsonQuery = bsons.size() == 0 ? null : "{$match: { $and: [" + String.join(",", bsons) + "] }}";
		
			Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mymongo", "reviewCol", bsonQuery);
		
			Dataset<ReviewUser> res = dataset.flatMap((FlatMapFunction<Row, ReviewUser>) r -> {
					List<ReviewUser> list_res = new ArrayList<ReviewUser>();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					Row nestedRow = null;
		
					boolean addedInList = false;
					Row r1 = r;
					ReviewUser reviewUser1 = new ReviewUser();
					reviewUser1.setR_author(new User());
					reviewUser1.setR_review1(new Review());
					
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Review.id for field idreview			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("idreview")) {
						if(nestedRow.getAs("idreview")==null)
							reviewUser1.getR_review1().setId(null);
						else{
							reviewUser1.getR_review1().setId((String)nestedRow.getAs("idreview"));
							toAdd1 = true;					
							}
					}
					// 	attribute Review.content for field content			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("content")) {
						if(nestedRow.getAs("content")==null)
							reviewUser1.getR_review1().setContent(null);
						else{
							reviewUser1.getR_review1().setContent((String)nestedRow.getAs("content"));
							toAdd1 = true;					
							}
					}
					// 	attribute Review.note for field note			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("note")) {
						if(nestedRow.getAs("note")==null)
							reviewUser1.getR_review1().setNote(null);
						else{
							reviewUser1.getR_review1().setNote((Integer)nestedRow.getAs("note"));
							toAdd1 = true;					
							}
					}
					// 	attribute User.id for field authorid			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("author");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("authorid")) {
						if(nestedRow.getAs("authorid")==null)
							reviewUser1.getR_author().setId(null);
						else{
							reviewUser1.getR_author().setId((String)nestedRow.getAs("authorid"));
							toAdd1 = true;					
							}
					}
					// 	attribute User.username for field username			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("author");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("username")) {
						if(nestedRow.getAs("username")==null)
							reviewUser1.getR_author().setUsername(null);
						else{
							reviewUser1.getR_author().setUsername((String)nestedRow.getAs("username"));
							toAdd1 = true;					
							}
					}
					// 	attribute User.city for field city			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("author");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("city")) {
						if(nestedRow.getAs("city")==null)
							reviewUser1.getR_author().setCity(null);
						else{
							reviewUser1.getR_author().setCity((String)nestedRow.getAs("city"));
							toAdd1 = true;					
							}
					}
					if(toAdd1) {
						
						list_res.add(reviewUser1);
						addedInList = true;
					} 
					
					
					return list_res.iterator();
		
			}, Encoders.bean(ReviewUser.class));
			// TODO drop duplicates based on roles ids
			//res= res.dropDuplicates(new String {});
			return res;
	}
	
	
	
	public java.util.List<pojo.ReviewUser> getReviewUserList(
		conditions.Condition<conditions.UserAttribute> r_author_condition,
		conditions.Condition<conditions.ReviewAttribute> r_review1_condition){
			//TODO
			return null;
		}
	
	public java.util.List<pojo.ReviewUser> getReviewUserListByR_authorCondition(
		conditions.Condition<conditions.UserAttribute> r_author_condition
	){
		return getReviewUserList(r_author_condition, null);
	}
	
	public java.util.List<pojo.ReviewUser> getReviewUserListByR_author(pojo.User r_author) {
		// TODO using id for selecting
		return null;
	}
	public java.util.List<pojo.ReviewUser> getReviewUserListByR_review1Condition(
		conditions.Condition<conditions.ReviewAttribute> r_review1_condition
	){
		return getReviewUserList(null, r_review1_condition);
	}
	
	public pojo.ReviewUser getReviewUserByR_review1(pojo.Review r_review1) {
		// TODO using id for selecting
		return null;
	}
	
	public void insertReviewUser(ReviewUser reviewUser){
		//TODO
	}
	
	public void deleteReviewUserList(
		conditions.Condition<conditions.UserAttribute> r_author_condition,
		conditions.Condition<conditions.ReviewAttribute> r_review1_condition){
			//TODO
		}
	
	public void deleteReviewUserListByR_authorCondition(
		conditions.Condition<conditions.UserAttribute> r_author_condition
	){
		deleteReviewUserList(r_author_condition, null);
	}
	
	public void deleteReviewUserListByR_author(pojo.User r_author) {
		// TODO using id for selecting
		return;
	}
	public void deleteReviewUserListByR_review1Condition(
		conditions.Condition<conditions.ReviewAttribute> r_review1_condition
	){
		deleteReviewUserList(null, r_review1_condition);
	}
	
	public void deleteReviewUserByR_review1(pojo.Review r_review1) {
		// TODO using id for selecting
		return;
	}
		
}
