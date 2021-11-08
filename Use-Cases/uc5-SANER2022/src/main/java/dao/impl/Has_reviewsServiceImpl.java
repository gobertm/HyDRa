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
import conditions.Has_reviewsAttribute;
import conditions.Operator;
import pojo.Has_reviews;
import tdo.FeedbackTDO;
import tdo.Has_reviewsTDO;
import pojo.Feedback;
import conditions.FeedbackAttribute;
import tdo.ProductTDO;
import tdo.Has_reviewsTDO;
import pojo.Product;
import conditions.ProductAttribute;
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

public class Has_reviewsServiceImpl extends dao.services.Has_reviewsService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Has_reviewsServiceImpl.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	
	
	// Left side 'prodid' of reference [product ]
	public Dataset<FeedbackTDO> getFeedbackTDOListReviewsInProductInFeedbackFromKvSchema(Condition<FeedbackAttribute> condition, MutableBoolean refilterFlag){	
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
		Dataset<FeedbackTDO> res = rows.map((MapFunction<Row, FeedbackTDO>) r -> {
					FeedbackTDO feedback_res = new FeedbackTDO();
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
					
					//Checking that reference field 'prodid' is mapped in Key
					if(fieldsListInKey.contains("prodid")){
						//Retrieving reference field 'prodid' in Key
						key = r.getAs("key");
						
						regex = finalKeypattern.replaceAll("\\*","(.*)");
						groupindex = fieldsListInKey.indexOf("prodid")+1;
						if(groupindex==null) {
							logger.warn("Attribute 'Feedback' mapped physical field 'prodid' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
						}
						p = Pattern.compile(regex);
						m = p.matcher(key);
						matches = m.find();
						String prodid = null;
						if(matches) {
						prodid = m.group(groupindex.intValue());
						} else {
						logger.warn("Cannot retrieve value 'prodid'. Probably due to an ambiguous regex.");
						feedback_res.addLogEvent("Cannot retrieve value for 'prodid' attribute stored in db redisbench. Probably due to an ambiguous regex.");
						}
						feedback_res.setKvSchema_feedback_product_prodid(prodid);
						
					}else {
						//Retrieving reference field 'prodid' in Value
						value = r.getAs("value");
						feedback_res.setKvSchema_feedback_product_prodid(value);
						}
						
					return feedback_res;
				}, Encoders.bean(FeedbackTDO.class));
		if(refilterFlag.booleanValue())
			res = res.filter((FilterFunction<FeedbackTDO>) r -> condition == null || condition.evaluate(r));
		res=res.dropDuplicates();
		return res;
	}
	
	// Right side 'asin' of reference [product ]
	public Dataset<ProductTDO> getProductTDOListReviewedProductInProductInFeedbackFromKvSchema(Condition<ProductAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = ProductServiceImpl.getSQLWhereClauseInProductTableFromMysqlbench(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", "'" + Util.escapeQuote(preparedValue) + "'");
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("mysqlbench", "productTable", where);
		
	
		Dataset<ProductTDO> res = d.map((MapFunction<Row, ProductTDO>) r -> {
					ProductTDO product_res = new ProductTDO();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Product.Id]
					String id = Util.getStringValue(r.getAs("asin"));
					product_res.setId(id);
					
					// attribute [Product.Title]
					String title = Util.getStringValue(r.getAs("title"));
					product_res.setTitle(title);
					
					// attribute [Product.Price]
					Double price = Util.getDoubleValue(r.getAs("price"));
					product_res.setPrice(price);
					
					// attribute [Product.Photo]
					String photo = Util.getStringValue(r.getAs("imgUrl"));
					product_res.setPhoto(photo);
	
					// Get reference column [asin ] for reference [product]
					String kvSchema_feedback_product_asin = r.getAs("asin") == null ? null : r.getAs("asin").toString();
					product_res.setKvSchema_feedback_product_asin(kvSchema_feedback_product_asin);
	
	
					return product_res;
				}, Encoders.bean(ProductTDO.class));
	
	
		return res;}
	
	
	
	
	
	
	public java.util.List<pojo.Has_reviews> getHas_reviewsList(
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition,
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition){
			//TODO
			return null;
		}
	
	public java.util.List<pojo.Has_reviews> getHas_reviewsListByReviewsCondition(
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition
	){
		return getHas_reviewsList(reviews_condition, null);
	}
	
	public java.util.List<pojo.Has_reviews> getHas_reviewsListByReviews(pojo.Feedback reviews) {
		// TODO using id for selecting
		return null;
	}
	public java.util.List<pojo.Has_reviews> getHas_reviewsListByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition
	){
		return getHas_reviewsList(null, reviewedProduct_condition);
	}
	
	public java.util.List<pojo.Has_reviews> getHas_reviewsListByReviewedProduct(pojo.Product reviewedProduct) {
		// TODO using id for selecting
		return null;
	}
	
	public void insertHas_reviews(Has_reviews has_reviews){
		//Link entities in join structures.
		// Update embedded structures mapped to non mandatory roles.
		// Update ref fields mapped to non mandatory roles. 
		insertHas_reviewsInRefStructFeedbackInRedisbench(has_reviews);
	}
	
	
	
	public 	boolean insertHas_reviewsInRefStructFeedbackInRedisbench(Has_reviews has_reviews){
	 	// Rel 'has_reviews' Insert in reference structure 'feedback'
		Feedback feedback = has_reviews.getReviews();
		Product product = has_reviews.getReviewedProduct();
	
		return false;
	}
	
	
	
	
	public void deleteHas_reviewsList(
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition,
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition){
			//TODO
		}
	
	public void deleteHas_reviewsListByReviewsCondition(
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition
	){
		deleteHas_reviewsList(reviews_condition, null);
	}
	
	public void deleteHas_reviewsListByReviews(pojo.Feedback reviews) {
		// TODO using id for selecting
		return;
	}
	public void deleteHas_reviewsListByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition
	){
		deleteHas_reviewsList(null, reviewedProduct_condition);
	}
	
	public void deleteHas_reviewsListByReviewedProduct(pojo.Product reviewedProduct) {
		// TODO using id for selecting
		return;
	}
		
}
