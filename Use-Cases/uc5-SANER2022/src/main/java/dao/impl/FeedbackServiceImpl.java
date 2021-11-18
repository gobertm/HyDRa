package dao.impl;

import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import org.apache.commons.lang3.StringUtils;
import util.Dataset;
import conditions.Condition;
import java.util.HashSet;
import java.util.Set;
import conditions.AndCondition;
import conditions.OrCondition;
import conditions.SimpleCondition;
import conditions.FeedbackAttribute;
import conditions.Operator;
import pojo.Feedback;
import tdo.ProductTDO;
import tdo.FeedbackTDO;
import pojo.Product;
import conditions.ProductAttribute;
import dao.services.ProductService;
import tdo.CustomerTDO;
import tdo.FeedbackTDO;
import pojo.Customer;
import conditions.CustomerAttribute;
import dao.services.CustomerService;
import java.util.List;
import java.util.ArrayList;
import util.ScalaUtil;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.commons.lang.mutable.MutableBoolean;
import util.Util;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import util.Row;
import org.apache.spark.sql.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import util.WrappedArray;
import org.apache.spark.api.java.function.FlatMapFunction;
import dbconnection.SparkConnectionMgr;
import dbconnection.DBConnectionMgr;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.ArrayType;
import static com.mongodb.client.model.Updates.addToSet;
import org.bson.Document;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;

public class FeedbackServiceImpl extends dao.services.FeedbackService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FeedbackServiceImpl.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	
	
	
	
	
	
	public Dataset<pojo.Feedback> getFeedbackList(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		conditions.Condition<conditions.FeedbackAttribute> feedback_condition
	){
		FeedbackServiceImpl feedbackService = this;
		ProductService productService = new ProductServiceImpl();  
		CustomerService customerService = new CustomerServiceImpl();
		MutableBoolean reviewedProduct_refilter = new MutableBoolean(false);
		List<Dataset<Feedback>> datasetsPOJO = new ArrayList<Dataset<Feedback>>();
		boolean all_already_persisted = false;
		MutableBoolean reviewer_refilter = new MutableBoolean(false);
		org.apache.spark.sql.Column joinCondition = null;
	
		
		Dataset<Feedback> res_feedback_reviewedProduct;
		Dataset<Product> res_Product;
		
		
		//Join datasets or return 
		Dataset<Feedback> res = fullOuterJoinsFeedback(datasetsPOJO);
		if(res == null)
			return null;
	
		Dataset<Product> lonelyReviewedProduct = null;
		Dataset<Customer> lonelyReviewer = null;
		
		List<Dataset<Product>> lonelyreviewedProductList = new ArrayList<Dataset<Product>>();
		lonelyreviewedProductList.add(productService.getProductListInProductsFromRedisModelB(reviewedProduct_condition, new MutableBoolean(false)));
		lonelyReviewedProduct = ProductService.fullOuterJoinsProduct(lonelyreviewedProductList);
		if(lonelyReviewedProduct != null) {
			res = fullLeftOuterJoinBetweenFeedbackAndReviewedProduct(res, lonelyReviewedProduct);
		}	
	
		List<Dataset<Customer>> lonelyreviewerList = new ArrayList<Dataset<Customer>>();
		lonelyreviewerList.add(customerService.getCustomerListInUserColFromMongoModelB(reviewer_condition, new MutableBoolean(false)));
		lonelyReviewer = CustomerService.fullOuterJoinsCustomer(lonelyreviewerList);
		if(lonelyReviewer != null) {
			res = fullLeftOuterJoinBetweenFeedbackAndReviewer(res, lonelyReviewer);
		}	
	
		
		if(reviewedProduct_refilter.booleanValue() || reviewer_refilter.booleanValue())
			res = res.filter((FilterFunction<Feedback>) r -> (reviewedProduct_condition == null || reviewedProduct_condition.evaluate(r.getReviewedProduct())) && (reviewer_condition == null || reviewer_condition.evaluate(r.getReviewer())));
		
	
		return res;
	
	}
	
	public Dataset<pojo.Feedback> getFeedbackListByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition
	){
		return getFeedbackList(reviewedProduct_condition, null, null);
	}
	
	public Dataset<pojo.Feedback> getFeedbackListByReviewedProduct(pojo.Product reviewedProduct) {
		conditions.Condition<conditions.ProductAttribute> cond = null;
		cond = conditions.Condition.simple(conditions.ProductAttribute.id, conditions.Operator.EQUALS, reviewedProduct.getId());
		Dataset<pojo.Feedback> res = getFeedbackListByReviewedProductCondition(cond);
	return res;
	}
	public Dataset<pojo.Feedback> getFeedbackListByReviewerCondition(
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition
	){
		return getFeedbackList(null, reviewer_condition, null);
	}
	
	public Dataset<pojo.Feedback> getFeedbackListByReviewer(pojo.Customer reviewer) {
		conditions.Condition<conditions.CustomerAttribute> cond = null;
		cond = conditions.Condition.simple(conditions.CustomerAttribute.id, conditions.Operator.EQUALS, reviewer.getId());
		Dataset<pojo.Feedback> res = getFeedbackListByReviewerCondition(cond);
	return res;
	}
	
	public Dataset<pojo.Feedback> getFeedbackListByFeedbackCondition(
		conditions.Condition<conditions.FeedbackAttribute> feedback_condition
	){
		return getFeedbackList(null, null, feedback_condition);
	}
	
	public void insertFeedback(Feedback feedback){
		//Link entities in join structures.
		// Update embedded structures mapped to non mandatory roles.
		// Update ref fields mapped to non mandatory roles. 
	}
	
	
	
	
	
	
	public void updateFeedbackList(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		conditions.Condition<conditions.FeedbackAttribute> feedback_condition,
		conditions.SetClause<conditions.FeedbackAttribute> set
	){
		//TODO
	}
	
	public void updateFeedbackListByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,
		conditions.SetClause<conditions.FeedbackAttribute> set
	){
		updateFeedbackList(reviewedProduct_condition, null, null, set);
	}
	
	public void updateFeedbackListByReviewedProduct(pojo.Product reviewedProduct, conditions.SetClause<conditions.FeedbackAttribute> set) {
		// TODO using id for selecting
		return;
	}
	public void updateFeedbackListByReviewerCondition(
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		conditions.SetClause<conditions.FeedbackAttribute> set
	){
		updateFeedbackList(null, reviewer_condition, null, set);
	}
	
	public void updateFeedbackListByReviewer(pojo.Customer reviewer, conditions.SetClause<conditions.FeedbackAttribute> set) {
		// TODO using id for selecting
		return;
	}
	
	public void updateFeedbackListByFeedbackCondition(
		conditions.Condition<conditions.FeedbackAttribute> feedback_condition,
		conditions.SetClause<conditions.FeedbackAttribute> set
	){
		updateFeedbackList(null, null, feedback_condition, set);
	}
	
	public void deleteFeedbackList(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		conditions.Condition<conditions.FeedbackAttribute> feedback_condition){
			//TODO
		}
	
	public void deleteFeedbackListByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition
	){
		deleteFeedbackList(reviewedProduct_condition, null, null);
	}
	
	public void deleteFeedbackListByReviewedProduct(pojo.Product reviewedProduct) {
		// TODO using id for selecting
		return;
	}
	public void deleteFeedbackListByReviewerCondition(
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition
	){
		deleteFeedbackList(null, reviewer_condition, null);
	}
	
	public void deleteFeedbackListByReviewer(pojo.Customer reviewer) {
		// TODO using id for selecting
		return;
	}
	
	public void deleteFeedbackListByFeedbackCondition(
		conditions.Condition<conditions.FeedbackAttribute> feedback_condition
	){
		deleteFeedbackList(null, null, feedback_condition);
	}
		
}
