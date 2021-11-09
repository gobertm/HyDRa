package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Feedback;
import conditions.*;
import dao.services.FeedbackService;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import util.Dataset;
import org.apache.spark.sql.Encoders;
import util.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.*;
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
import util.WrappedArray;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FilterFunction;
import java.util.ArrayList;
import org.apache.commons.lang.mutable.MutableBoolean;
import tdo.*;
import pojo.*;
import util.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.ArrayType;
import scala.Tuple2;
import org.bson.Document;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;


public class FeedbackServiceImpl extends FeedbackService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FeedbackServiceImpl.class);
	
	
	
	
	
	//TODO redis
	public Dataset<Feedback> getFeedbackListInFeedbackFromRedisbench(conditions.Condition<conditions.FeedbackAttribute> condition, MutableBoolean refilterFlag){
		// Build the key pattern
		//  - If the condition attribute is in the key pattern, replace by the value. Only if operator is EQUALS.
		//  - Replace all other fields of key pattern by a '*' 
		String keypattern= "", keypatternAllVariables="";
		String valueCond=null;
		String finalKeypattern;
		List<String> fieldsListInKey = new ArrayList<>();
		Set<FeedbackAttribute> keyAttributes = new HashSet<>();
		if(!Util.containsOrCondition(condition)){
			valueCond=Util.getStringValue(Util.getValueOfAttributeInEqualCondition(condition,FeedbackAttribute.product));
			keyAttributes.add(FeedbackAttribute.product);
		}
		else{
			valueCond=null;
			refilterFlag.setValue(true);
		}
		if(valueCond==null)
			keypattern=keypattern.concat("*");
		else
			keypattern=keypattern.concat(valueCond);
		fieldsListInKey.add("prodid");
		keypatternAllVariables=keypatternAllVariables.concat("*");
		keypattern=keypattern.concat(":");
		keypatternAllVariables=keypatternAllVariables.concat(":");
		if(!Util.containsOrCondition(condition)){
			valueCond=Util.getStringValue(Util.getValueOfAttributeInEqualCondition(condition,FeedbackAttribute.customer));
			keyAttributes.add(FeedbackAttribute.customer);
		}
		else{
			valueCond=null;
			refilterFlag.setValue(true);
		}
		if(valueCond==null)
			keypattern=keypattern.concat("*");
		else
			keypattern=keypattern.concat(valueCond);
		fieldsListInKey.add("customerid");
		keypatternAllVariables=keypatternAllVariables.concat("*");
		if(!refilterFlag.booleanValue()){
			Set<FeedbackAttribute> conditionAttributes = Util.getConditionAttributes(condition);
			for (FeedbackAttribute a : conditionAttributes) {
				if (!keyAttributes.contains(a)) {
					refilterFlag.setValue(true);
					break;
				}
			}
		}
			
		// Find the type of query to perform in order to retrieve a Dataset<Row>
		// Based on the type of the value. Is a it a simple string or a hash or a list... 
		Dataset<Row> rows;
		rows = SparkConnectionMgr.getRowsFromKeyValue("redisbench",keypattern);
		if(rows == null || rows.isEmpty())
				return null;
		// Transform to POJO. Based on Row containing (String key, String value)
		finalKeypattern = keypatternAllVariables;
		Dataset<Feedback> res = rows.map((MapFunction<Row, Feedback>) r -> {
					Feedback feedback_res = new Feedback();
					Integer groupindex = null;
					String regex = null;
					String value = null;
					Pattern p, pattern = null;
					Matcher m, match = null;
					String key="";
					boolean matches = false;
					// attribute [Feedback.Rate]
					// Attribute mapped in value part.
					value = r.getAs("value");
					regex = "(.*)(&&)(.*)";
					groupindex = 1;
					if(groupindex == null) {
						logger.warn("Cannot retrieve value for Feedbackrate attribute stored in db redisbench. Probably due to an ambiguous regex.");
						feedback_res.addLogEvent("Cannot retrieve value for Feedback.rate attribute stored in db redisbench. Probably due to an ambiguous regex.");
					}
					p = Pattern.compile(regex);
					m = p.matcher(value);
					matches = m.find();
					String rate = null;
					if(matches) {
						rate = m.group(groupindex.intValue());
					} else {
						logger.warn("Cannot retrieve value for Feedbackrate attribute stored in db redisbench. Probably due to an ambiguous regex.");
						feedback_res.addLogEvent("Cannot retrieve value for Feedback.rate attribute stored in db redisbench. Probably due to an ambiguous regex.");
					}
					feedback_res.setRate(rate == null ? null : Double.parseDouble(rate));
					// attribute [Feedback.Content]
					// Attribute mapped in value part.
					value = r.getAs("value");
					regex = "(.*)(&&)(.*)";
					groupindex = 3;
					if(groupindex == null) {
						logger.warn("Cannot retrieve value for Feedbackcontent attribute stored in db redisbench. Probably due to an ambiguous regex.");
						feedback_res.addLogEvent("Cannot retrieve value for Feedback.content attribute stored in db redisbench. Probably due to an ambiguous regex.");
					}
					p = Pattern.compile(regex);
					m = p.matcher(value);
					matches = m.find();
					String content = null;
					if(matches) {
						content = m.group(groupindex.intValue());
					} else {
						logger.warn("Cannot retrieve value for Feedbackcontent attribute stored in db redisbench. Probably due to an ambiguous regex.");
						feedback_res.addLogEvent("Cannot retrieve value for Feedback.content attribute stored in db redisbench. Probably due to an ambiguous regex.");
					}
					feedback_res.setContent(content == null ? null : content);
					// attribute [Feedback.Product]
					// Attribute mapped in a key.
					key = r.getAs("key");
					regex = finalKeypattern.replaceAll("\\*","(.*)");
					groupindex = fieldsListInKey.indexOf("prodid")+1;
					if(groupindex==null) {
						logger.warn("Attribute of 'Feedback' mapped physical field 'prodid' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
					}
					p = Pattern.compile(regex);
					m = p.matcher(key);
					matches = m.find();
					String product = null;
					if(matches) {
						product = m.group(groupindex.intValue());
					} else {
						logger.warn("Cannot retrieve value for Feedbackproduct attribute stored in db redisbench. Probably due to an ambiguous regex.");
						feedback_res.addLogEvent("Cannot retrieve value for Feedback.product attribute stored in db redisbench. Probably due to an ambiguous regex.");
					}
					feedback_res.setProduct(product == null ? null : product);
					// attribute [Feedback.Customer]
					// Attribute mapped in a key.
					key = r.getAs("key");
					regex = finalKeypattern.replaceAll("\\*","(.*)");
					groupindex = fieldsListInKey.indexOf("customerid")+1;
					if(groupindex==null) {
						logger.warn("Attribute of 'Feedback' mapped physical field 'customerid' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
					}
					p = Pattern.compile(regex);
					m = p.matcher(key);
					matches = m.find();
					String customer = null;
					if(matches) {
						customer = m.group(groupindex.intValue());
					} else {
						logger.warn("Cannot retrieve value for Feedbackcustomer attribute stored in db redisbench. Probably due to an ambiguous regex.");
						feedback_res.addLogEvent("Cannot retrieve value for Feedback.customer attribute stored in db redisbench. Probably due to an ambiguous regex.");
					}
					feedback_res.setCustomer(customer == null ? null : customer);
					
						
					return feedback_res;
				}, Encoders.bean(Feedback.class));
		if(refilterFlag.booleanValue())
			res = res.filter((FilterFunction<Feedback>) r -> condition == null || condition.evaluate(r));
		res=res.dropDuplicates();
		return res;
		
	}
	
	
	// TODO get based on id(s). Ex:public Client getClientById(Long id)
	
	public Dataset<Feedback> getFeedbackListByRate(Double rate) {
		return getFeedbackList(conditions.Condition.simple(conditions.FeedbackAttribute.rate, conditions.Operator.EQUALS, rate));
	}
	
	public Dataset<Feedback> getFeedbackListByContent(String content) {
		return getFeedbackList(conditions.Condition.simple(conditions.FeedbackAttribute.content, conditions.Operator.EQUALS, content));
	}
	
	public Dataset<Feedback> getFeedbackListByProduct(String product) {
		return getFeedbackList(conditions.Condition.simple(conditions.FeedbackAttribute.product, conditions.Operator.EQUALS, product));
	}
	
	public Dataset<Feedback> getFeedbackListByCustomer(String customer) {
		return getFeedbackList(conditions.Condition.simple(conditions.FeedbackAttribute.customer, conditions.Operator.EQUALS, customer));
	}
	
	
	
	
	public Dataset<Feedback> getReviewListInWrite(conditions.Condition<conditions.FeedbackAttribute> review_condition,conditions.Condition<conditions.CustomerAttribute> reviewer_condition)		{
		MutableBoolean review_refilter = new MutableBoolean(false);
		List<Dataset<Feedback>> datasetsPOJO = new ArrayList<Dataset<Feedback>>();
		Dataset<Customer> all = null;
		boolean all_already_persisted = false;
		MutableBoolean reviewer_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// For role 'review' in reference 'customer1'. A->B Scenario
		reviewer_refilter = new MutableBoolean(false);
		Dataset<FeedbackTDO> feedbackTDOcustomer1review = writeService.getFeedbackTDOListReviewInCustomer1InFeedbackFromKvSchema(review_condition, review_refilter);
		Dataset<CustomerTDO> customerTDOcustomer1reviewer = writeService.getCustomerTDOListReviewerInCustomer1InFeedbackFromKvSchema(reviewer_condition, reviewer_refilter);
		if(reviewer_refilter.booleanValue()) {
			if(all == null)
				all = new CustomerServiceImpl().getCustomerList(reviewer_condition);
			joinCondition = null;
			joinCondition = customerTDOcustomer1reviewer.col("id").equalTo(all.col("id"));
			if(joinCondition == null)
				customerTDOcustomer1reviewer = customerTDOcustomer1reviewer.as("A").join(all).select("A.*").as(Encoders.bean(CustomerTDO.class));
			else
				customerTDOcustomer1reviewer = customerTDOcustomer1reviewer.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(CustomerTDO.class));
		}
	
		
		Dataset<Row> res_customer1 = feedbackTDOcustomer1review.join(customerTDOcustomer1reviewer
				.withColumnRenamed("id", "Customer_id")
				.withColumnRenamed("firstname", "Customer_firstname")
				.withColumnRenamed("lastname", "Customer_lastname")
				.withColumnRenamed("gender", "Customer_gender")
				.withColumnRenamed("birthday", "Customer_birthday")
				.withColumnRenamed("creationDate", "Customer_creationDate")
				.withColumnRenamed("locationip", "Customer_locationip")
				.withColumnRenamed("browser", "Customer_browser")
				.withColumnRenamed("logEvents", "Customer_logEvents"),
				feedbackTDOcustomer1review.col("kvSchema_feedback_customer1_customerid").equalTo(customerTDOcustomer1reviewer.col("kvSchema_feedback_customer1_id")));
		Dataset<Feedback> res_Feedback_customer1 = res_customer1.select( "rate", "content", "product", "customer", "logEvents").as(Encoders.bean(Feedback.class));
		
		logger.warn("Conceptual entity 'Feedback' does not have a declared identifier. Returned results may be inconsistent.");
		res_Feedback_customer1 = res_Feedback_customer1.dropDuplicates();
		datasetsPOJO.add(res_Feedback_customer1);
		
		
		Dataset<Write> res_write_review;
		Dataset<Feedback> res_Feedback;
		
		
		//Join datasets or return 
		Dataset<Feedback> res = fullOuterJoinsFeedback(datasetsPOJO);
		if(res == null)
			return null;
	
		if(review_refilter.booleanValue())
			res = res.filter((FilterFunction<Feedback>) r -> review_condition == null || review_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Feedback> getReviewListInWriteByReviewCondition(conditions.Condition<conditions.FeedbackAttribute> review_condition){
		return getReviewListInWrite(review_condition, null);
	}
	public Dataset<Feedback> getReviewListInWriteByReviewerCondition(conditions.Condition<conditions.CustomerAttribute> reviewer_condition){
		return getReviewListInWrite(null, reviewer_condition);
	}
	
	public Dataset<Feedback> getReviewListInWriteByReviewer(pojo.Customer reviewer){
		if(reviewer == null)
			return null;
	
		Condition c;
		c=Condition.simple(CustomerAttribute.id,Operator.EQUALS, reviewer.getId());
		Dataset<Feedback> res = getReviewListInWriteByReviewerCondition(c);
		return res;
	}
	
	public Dataset<Feedback> getReviewsListInHas_reviews(conditions.Condition<conditions.FeedbackAttribute> reviews_condition,conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition)		{
		MutableBoolean reviews_refilter = new MutableBoolean(false);
		List<Dataset<Feedback>> datasetsPOJO = new ArrayList<Dataset<Feedback>>();
		Dataset<Product> all = null;
		boolean all_already_persisted = false;
		MutableBoolean reviewedProduct_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// For role 'reviews' in reference 'product'. A->B Scenario
		reviewedProduct_refilter = new MutableBoolean(false);
		Dataset<FeedbackTDO> feedbackTDOproductreviews = has_reviewsService.getFeedbackTDOListReviewsInProductInFeedbackFromKvSchema(reviews_condition, reviews_refilter);
		Dataset<ProductTDO> productTDOproductreviewedProduct = has_reviewsService.getProductTDOListReviewedProductInProductInFeedbackFromKvSchema(reviewedProduct_condition, reviewedProduct_refilter);
		if(reviewedProduct_refilter.booleanValue()) {
			if(all == null)
				all = new ProductServiceImpl().getProductList(reviewedProduct_condition);
			joinCondition = null;
			joinCondition = productTDOproductreviewedProduct.col("id").equalTo(all.col("id"));
			if(joinCondition == null)
				productTDOproductreviewedProduct = productTDOproductreviewedProduct.as("A").join(all).select("A.*").as(Encoders.bean(ProductTDO.class));
			else
				productTDOproductreviewedProduct = productTDOproductreviewedProduct.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(ProductTDO.class));
		}
	
		
		Dataset<Row> res_product = feedbackTDOproductreviews.join(productTDOproductreviewedProduct
				.withColumnRenamed("id", "Product_id")
				.withColumnRenamed("title", "Product_title")
				.withColumnRenamed("price", "Product_price")
				.withColumnRenamed("photo", "Product_photo")
				.withColumnRenamed("logEvents", "Product_logEvents"),
				feedbackTDOproductreviews.col("kvSchema_feedback_product_prodid").equalTo(productTDOproductreviewedProduct.col("kvSchema_feedback_product_asin")));
		Dataset<Feedback> res_Feedback_product = res_product.select( "rate", "content", "product", "customer", "logEvents").as(Encoders.bean(Feedback.class));
		
		logger.warn("Conceptual entity 'Feedback' does not have a declared identifier. Returned results may be inconsistent.");
		res_Feedback_product = res_Feedback_product.dropDuplicates();
		datasetsPOJO.add(res_Feedback_product);
		
		
		Dataset<Has_reviews> res_has_reviews_reviews;
		Dataset<Feedback> res_Feedback;
		
		
		//Join datasets or return 
		Dataset<Feedback> res = fullOuterJoinsFeedback(datasetsPOJO);
		if(res == null)
			return null;
	
		if(reviews_refilter.booleanValue())
			res = res.filter((FilterFunction<Feedback>) r -> reviews_condition == null || reviews_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Feedback> getReviewsListInHas_reviewsByReviewsCondition(conditions.Condition<conditions.FeedbackAttribute> reviews_condition){
		return getReviewsListInHas_reviews(reviews_condition, null);
	}
	public Dataset<Feedback> getReviewsListInHas_reviewsByReviewedProductCondition(conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition){
		return getReviewsListInHas_reviews(null, reviewedProduct_condition);
	}
	
	public Dataset<Feedback> getReviewsListInHas_reviewsByReviewedProduct(pojo.Product reviewedProduct){
		if(reviewedProduct == null)
			return null;
	
		Condition c;
		c=Condition.simple(ProductAttribute.id,Operator.EQUALS, reviewedProduct.getId());
		Dataset<Feedback> res = getReviewsListInHas_reviewsByReviewedProductCondition(c);
		return res;
	}
	
	
	
	public boolean insertFeedback(Feedback feedback){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertFeedbackInFeedbackFromRedisbench(feedback) || inserted ;
		return inserted;
	}
	
	public boolean insertFeedbackInFeedbackFromRedisbench(Feedback feedback)	{
		Condition<FeedbackAttribute> conditionID;
		String idvalue="";
		boolean entityExists=false;
				
		if(!entityExists){
			String key="";
			boolean toAdd = false;
			key += feedback.getProduct();
			key += ":";
			key += feedback.getCustomer();
			
			String value="";
			if(feedback.getRate()!=null){
				toAdd = true;
				value += feedback.getRate();
			}
			else
				value+= "";
			value += "&&";
			if(feedback.getContent()!=null){
				toAdd = true;
				value += feedback.getContent();
			}
			else
				value+= "";
			//No addition of key value pair when the value is null.
			if(toAdd)
				SparkConnectionMgr.writeKeyValue(key,value,"redisbench");
	
			logger.info("Inserted [Feedback] entity ID [{}] in [Feedback] in database [Redisbench]", idvalue);
		}
		else
			logger.warn("[Feedback] entity ID [{}] already present in [Feedback] in database [Redisbench]", idvalue);
		return !entityExists;
	} 
	
	public void updateFeedbackList(conditions.Condition<conditions.FeedbackAttribute> condition, conditions.SetClause<conditions.FeedbackAttribute> set){
		//TODO
	}
	
	public void updateFeedback(pojo.Feedback feedback) {
		//TODO using the id
		return;
	}
	public void updateReviewListInWrite(
		conditions.Condition<conditions.FeedbackAttribute> review_condition,
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		
		conditions.SetClause<conditions.FeedbackAttribute> set
	){
		//TODO
	}
	
	public void updateReviewListInWriteByReviewCondition(
		conditions.Condition<conditions.FeedbackAttribute> review_condition,
		conditions.SetClause<conditions.FeedbackAttribute> set
	){
		updateReviewListInWrite(review_condition, null, set);
	}
	public void updateReviewListInWriteByReviewerCondition(
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		conditions.SetClause<conditions.FeedbackAttribute> set
	){
		updateReviewListInWrite(null, reviewer_condition, set);
	}
	
	public void updateReviewListInWriteByReviewer(
		pojo.Customer reviewer,
		conditions.SetClause<conditions.FeedbackAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateReviewsListInHas_reviews(
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition,
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,
		
		conditions.SetClause<conditions.FeedbackAttribute> set
	){
		//TODO
	}
	
	public void updateReviewsListInHas_reviewsByReviewsCondition(
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition,
		conditions.SetClause<conditions.FeedbackAttribute> set
	){
		updateReviewsListInHas_reviews(reviews_condition, null, set);
	}
	public void updateReviewsListInHas_reviewsByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,
		conditions.SetClause<conditions.FeedbackAttribute> set
	){
		updateReviewsListInHas_reviews(null, reviewedProduct_condition, set);
	}
	
	public void updateReviewsListInHas_reviewsByReviewedProduct(
		pojo.Product reviewedProduct,
		conditions.SetClause<conditions.FeedbackAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	
	
	public void deleteFeedbackList(conditions.Condition<conditions.FeedbackAttribute> condition){
		//TODO
	}
	
	public void deleteFeedback(pojo.Feedback feedback) {
		//TODO using the id
		return;
	}
	public void deleteReviewListInWrite(	
		conditions.Condition<conditions.FeedbackAttribute> review_condition,	
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition){
			//TODO
		}
	
	public void deleteReviewListInWriteByReviewCondition(
		conditions.Condition<conditions.FeedbackAttribute> review_condition
	){
		deleteReviewListInWrite(review_condition, null);
	}
	public void deleteReviewListInWriteByReviewerCondition(
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition
	){
		deleteReviewListInWrite(null, reviewer_condition);
	}
	
	public void deleteReviewListInWriteByReviewer(
		pojo.Customer reviewer 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteReviewsListInHas_reviews(	
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition,	
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition){
			//TODO
		}
	
	public void deleteReviewsListInHas_reviewsByReviewsCondition(
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition
	){
		deleteReviewsListInHas_reviews(reviews_condition, null);
	}
	public void deleteReviewsListInHas_reviewsByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition
	){
		deleteReviewsListInHas_reviews(null, reviewedProduct_condition);
	}
	
	public void deleteReviewsListInHas_reviewsByReviewedProduct(
		pojo.Product reviewedProduct 
	){
		//TODO get id in condition
		return;	
	}
	
	
}
