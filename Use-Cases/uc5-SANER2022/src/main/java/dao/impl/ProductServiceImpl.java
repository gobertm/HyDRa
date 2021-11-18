package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Product;
import conditions.*;
import dao.services.ProductService;
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


public class ProductServiceImpl extends ProductService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProductServiceImpl.class);
	
	
	
	
	
	
	//TODO redis
	public Dataset<Product> getProductListInProductsFromRedisModelB(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag){
		// Build the key pattern
		//  - If the condition attribute is in the key pattern, replace by the value. Only if operator is EQUALS.
		//  - Replace all other fields of key pattern by a '*' 
		String keypattern= "", keypatternAllVariables="";
		String valueCond=null;
		String finalKeypattern;
		List<String> fieldsListInKey = new ArrayList<>();
		Set<ProductAttribute> keyAttributes = new HashSet<>();
		keypattern=keypattern.concat("PRODUCT:");
		keypatternAllVariables=keypatternAllVariables.concat("PRODUCT:");
		if(!Util.containsOrCondition(condition)){
			valueCond=Util.getStringValue(Util.getValueOfAttributeInEqualCondition(condition,ProductAttribute.id));
			keyAttributes.add(ProductAttribute.id);
		}
		else{
			valueCond=null;
			refilterFlag.setValue(true);
		}
		if(valueCond==null)
			keypattern=keypattern.concat("*");
		else
			keypattern=keypattern.concat(valueCond);
		fieldsListInKey.add("asin");
		keypatternAllVariables=keypatternAllVariables.concat("*");
		if(!refilterFlag.booleanValue()){
			Set<ProductAttribute> conditionAttributes = Util.getConditionAttributes(condition);
			for (ProductAttribute a : conditionAttributes) {
				if (!keyAttributes.contains(a)) {
					refilterFlag.setValue(true);
					break;
				}
			}
		}
			
		// Find the type of query to perform in order to retrieve a Dataset<Row>
		// Based on the type of the value. Is a it a simple string or a hash or a list... 
		Dataset<Row> rows;
		StructType structType = new StructType(new StructField[] {
			DataTypes.createStructField("_id", DataTypes.StringType, true), //technical field to store the key.
			DataTypes.createStructField("asin", DataTypes.StringType, true)
	,		DataTypes.createStructField("title", DataTypes.StringType, true)
	,		DataTypes.createStructField("price", DataTypes.StringType, true)
	,		DataTypes.createStructField("imgUrl", DataTypes.StringType, true)
		});
		rows = SparkConnectionMgr.getRowsFromKeyValueHashes("redisModelB",keypattern, structType);
		if(rows == null || rows.isEmpty())
				return null;
		boolean isStriped = (StringUtils.countMatches(keypattern, "*") == 1 && keypattern.endsWith("*"));
		String prefix=isStriped?keypattern.substring(0, keypattern.length() - 1):"";
		finalKeypattern = keypatternAllVariables;
		Dataset<Product> res = rows.map((MapFunction<Row, Product>) r -> {
					Product product_res = new Product();
					Integer groupindex = null;
					String regex = null;
					String value = null;
					Pattern p, pattern = null;
					Matcher m, match = null;
					boolean matches = false;
					String key = isStriped ? prefix + r.getAs("_id") : r.getAs("_id");
					// Spark Redis automatically strips leading character if the pattern provided contains a single '*' at the end.				
					pattern = Pattern.compile("\\*");
			        match = pattern.matcher(finalKeypattern);
					regex = finalKeypattern.replaceAll("\\*","(.*)");
					p = Pattern.compile(regex);
					m = p.matcher(key);
					matches = m.find();
					// attribute [Product.Id]
					// Attribute mapped in a key.
					groupindex = fieldsListInKey.indexOf("asin")+1;
					if(groupindex==null) {
						logger.warn("Attribute 'Product' mapped physical field 'asin' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
					}
					String id = null;
					if(matches) {
						id = m.group(groupindex.intValue());
					} else {
						logger.warn("Cannot retrieve value for Productid attribute stored in db redisModelB. Probably due to an ambiguous regex.");
						product_res.addLogEvent("Cannot retrieve value for Product.id attribute stored in db redisModelB. Probably due to an ambiguous regex.");
					}
					product_res.setId(id == null ? null : id);
					// attribute [Product.Title]
					String title = r.getAs("title") == null ? null : r.getAs("title");
					product_res.setTitle(title);
					// attribute [Product.Price]
					Double price = r.getAs("price") == null ? null : Double.parseDouble(r.getAs("price"));
					product_res.setPrice(price);
					// attribute [Product.Photo]
					String photo = r.getAs("imgUrl") == null ? null : r.getAs("imgUrl");
					product_res.setPhoto(photo);
	
						return product_res;
				}, Encoders.bean(Product.class));
		if(refilterFlag.booleanValue())
			res = res.filter((FilterFunction<Product>) r -> condition == null || condition.evaluate(r));
		res=res.dropDuplicates(new String[] {"id"});
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Product> getReviewedProductListInFeedback(conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,conditions.Condition<conditions.CustomerAttribute> reviewer_condition, conditions.Condition<conditions.FeedbackAttribute> feedback_condition)		{
		MutableBoolean reviewedProduct_refilter = new MutableBoolean(false);
		List<Dataset<Product>> datasetsPOJO = new ArrayList<Dataset<Product>>();
		Dataset<Customer> all = null;
		boolean all_already_persisted = false;
		MutableBoolean reviewer_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		
		Dataset<Feedback> res_feedback_reviewedProduct;
		Dataset<Product> res_Product;
		
		
		//Join datasets or return 
		Dataset<Product> res = fullOuterJoinsProduct(datasetsPOJO);
		if(res == null)
			return null;
	
		List<Dataset<Product>> lonelyProductList = new ArrayList<Dataset<Product>>();
		lonelyProductList.add(getProductListInProductsFromRedisModelB(reviewedProduct_condition, new MutableBoolean(false)));
		Dataset<Product> lonelyProduct = fullOuterJoinsProduct(lonelyProductList);
		if(lonelyProduct != null) {
			res = fullLeftOuterJoinsProduct(Arrays.asList(res, lonelyProduct));
		}
		if(reviewedProduct_refilter.booleanValue())
			res = res.filter((FilterFunction<Product>) r -> reviewedProduct_condition == null || reviewedProduct_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Product> getOrderedProductsListInComposed_of(conditions.Condition<conditions.OrderAttribute> orderP_condition,conditions.Condition<conditions.ProductAttribute> orderedProducts_condition)		{
		MutableBoolean orderedProducts_refilter = new MutableBoolean(false);
		List<Dataset<Product>> datasetsPOJO = new ArrayList<Dataset<Product>>();
		Dataset<Order> all = null;
		boolean all_already_persisted = false;
		MutableBoolean orderP_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// join physical structure A<-AB->B
		// (A) (AB) (B)  OR (A B) (AB) Join table is 'alone'
		orderP_refilter = new MutableBoolean(false);
		Dataset<ProductTDO> productTDOproductReforderedProducts = composed_ofService.getProductTDOListOrderedProductsInProductRefInProductsFromKvSchemaB(orderedProducts_condition, orderedProducts_refilter);
		Dataset<Composed_ofTDO> composed_ofTDOproductRef_orderRef = composed_ofService.getComposed_ofTDOListInComposed_of_ProductRef_OrderRefInDetailOrderColFromMongoSchemaB();
		Dataset<OrderTDO> orderTDOproductReforderP = composed_ofService.getOrderTDOListOrderPInOrderRefInOrderTableFromRelSchemaB(orderP_condition, orderP_refilter);
		if(orderP_refilter.booleanValue()) {
			if(all == null)
					all = new OrderServiceImpl().getOrderList(orderP_condition);
			joinCondition = null;
				joinCondition = orderTDOproductReforderP.col("id").equalTo(all.col("id"));
				orderTDOproductReforderP = orderTDOproductReforderP.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(OrderTDO.class));
		}
		Dataset<Row> res_productRef = productTDOproductReforderedProducts.join(composed_ofTDOproductRef_orderRef.withColumnRenamed("logEvents", "composed_of_logEvents"),
										productTDOproductReforderedProducts.col("mongoSchemaB_detailOrderCol_productRef_asin").equalTo(composed_ofTDOproductRef_orderRef.col("mongoSchemaB_detailOrderCol_productRef_productid")));
		res_productRef = res_productRef.join(orderTDOproductReforderP
			.withColumnRenamed("id", "Order_id")
			.withColumnRenamed("orderdate", "Order_orderdate")
			.withColumnRenamed("totalprice", "Order_totalprice")
			.withColumnRenamed("logEvents", "Order_logEvents"),
			res_productRef.col("mongoSchemaB_detailOrderCol_orderRef_orderid").equalTo(orderTDOproductReforderP.col("mongoSchemaB_detailOrderCol_orderRef_orderId")));
		Dataset<Product> res_Product_productRef = res_productRef.select( "id", "title", "price", "photo", "logEvents").as(Encoders.bean(Product.class));
		datasetsPOJO.add(res_Product_productRef.dropDuplicates(new String[] {"id"}));	
		
		
		
		Dataset<Composed_of> res_composed_of_orderedProducts;
		Dataset<Product> res_Product;
		
		
		//Join datasets or return 
		Dataset<Product> res = fullOuterJoinsProduct(datasetsPOJO);
		if(res == null)
			return null;
	
		if(orderedProducts_refilter.booleanValue())
			res = res.filter((FilterFunction<Product>) r -> orderedProducts_condition == null || orderedProducts_condition.evaluate(r));
		
	
		return res;
		}
	
	
	public boolean insertProduct(Product product){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertProductInProductsFromRedisModelB(product) || inserted ;
		return inserted;
	}
	
	public boolean insertProductInProductsFromRedisModelB(Product product)	{
		Condition<ProductAttribute> conditionID;
		String idvalue="";
		boolean entityExists=false;
		conditionID = Condition.simple(ProductAttribute.id, Operator.EQUALS, product.getId());
		idvalue+=product.getId();
		Dataset res = getProductListInProductsFromRedisModelB(conditionID,new MutableBoolean(false));
		entityExists = res != null && !res.isEmpty();
				
		if(!entityExists){
			String key="";
			boolean toAdd = false;
			key += "PRODUCT:";
			key += product.getId();
			List<Tuple2<String,String>> hash = new ArrayList<>();
			toAdd = false;
			String fieldname_asin="asin";
			String value_asin="";
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(fieldname_asin,value_asin));
			toAdd = false;
			String fieldname_title="title";
			String value_title="";
			if(product.getTitle()!=null){
				toAdd = true;
				value_title += product.getTitle();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(fieldname_title,value_title));
			toAdd = false;
			String fieldname_price="price";
			String value_price="";
			if(product.getPrice()!=null){
				toAdd = true;
				value_price += product.getPrice();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(fieldname_price,value_price));
			toAdd = false;
			String fieldname_imgUrl="imgUrl";
			String value_imgUrl="";
			if(product.getPhoto()!=null){
				toAdd = true;
				value_imgUrl += product.getPhoto();
			}
			// When value is null for a field in the hash we dont add it to the hash.
			if(toAdd)
				hash.add(new Tuple2<String,String>(fieldname_imgUrl,value_imgUrl));
			SparkConnectionMgr.writeKeyValueHash(key,hash, "redisModelB");
	
			logger.info("Inserted [Product] entity ID [{}] in [Products] in database [RedisModelB]", idvalue);
		}
		else
			logger.warn("[Product] entity ID [{}] already present in [Products] in database [RedisModelB]", idvalue);
		return !entityExists;
	} 
	
	public void updateProductList(conditions.Condition<conditions.ProductAttribute> condition, conditions.SetClause<conditions.ProductAttribute> set){
		//TODO
	}
	
	public void updateProduct(pojo.Product product) {
		//TODO using the id
		return;
	}
	public void updateReviewedProductListInFeedback(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		conditions.Condition<conditions.FeedbackAttribute> feedback,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		//TODO
	}
	
	public void updateReviewedProductListInFeedbackByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateReviewedProductListInFeedback(reviewedProduct_condition, null, null, set);
	}
	public void updateReviewedProductListInFeedbackByReviewerCondition(
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateReviewedProductListInFeedback(null, reviewer_condition, null, set);
	}
	
	public void updateReviewedProductListInFeedbackByReviewer(
		pojo.Customer reviewer,
		conditions.SetClause<conditions.ProductAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateReviewedProductListInFeedbackByFeedbackCondition(
		conditions.Condition<conditions.FeedbackAttribute> feedback_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateReviewedProductListInFeedback(null, null, feedback_condition, set);
	}
	public void updateOrderedProductsListInComposed_of(
		conditions.Condition<conditions.OrderAttribute> orderP_condition,
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition,
		
		conditions.SetClause<conditions.ProductAttribute> set
	){
		//TODO
	}
	
	public void updateOrderedProductsListInComposed_ofByOrderPCondition(
		conditions.Condition<conditions.OrderAttribute> orderP_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateOrderedProductsListInComposed_of(orderP_condition, null, set);
	}
	
	public void updateOrderedProductsListInComposed_ofByOrderP(
		pojo.Order orderP,
		conditions.SetClause<conditions.ProductAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateOrderedProductsListInComposed_ofByOrderedProductsCondition(
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateOrderedProductsListInComposed_of(null, orderedProducts_condition, set);
	}
	
	
	public void deleteProductList(conditions.Condition<conditions.ProductAttribute> condition){
		//TODO
	}
	
	public void deleteProduct(pojo.Product product) {
		//TODO using the id
		return;
	}
	public void deleteReviewedProductListInFeedback(	
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,	
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		conditions.Condition<conditions.FeedbackAttribute> feedback){
			//TODO
		}
	
	public void deleteReviewedProductListInFeedbackByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition
	){
		deleteReviewedProductListInFeedback(reviewedProduct_condition, null, null);
	}
	public void deleteReviewedProductListInFeedbackByReviewerCondition(
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition
	){
		deleteReviewedProductListInFeedback(null, reviewer_condition, null);
	}
	
	public void deleteReviewedProductListInFeedbackByReviewer(
		pojo.Customer reviewer 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteReviewedProductListInFeedbackByFeedbackCondition(
		conditions.Condition<conditions.FeedbackAttribute> feedback_condition
	){
		deleteReviewedProductListInFeedback(null, null, feedback_condition);
	}
	public void deleteOrderedProductsListInComposed_of(	
		conditions.Condition<conditions.OrderAttribute> orderP_condition,	
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition){
			//TODO
		}
	
	public void deleteOrderedProductsListInComposed_ofByOrderPCondition(
		conditions.Condition<conditions.OrderAttribute> orderP_condition
	){
		deleteOrderedProductsListInComposed_of(orderP_condition, null);
	}
	
	public void deleteOrderedProductsListInComposed_ofByOrderP(
		pojo.Order orderP 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteOrderedProductsListInComposed_ofByOrderedProductsCondition(
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition
	){
		deleteOrderedProductsListInComposed_of(null, orderedProducts_condition);
	}
	
}
