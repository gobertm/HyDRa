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
	
	
	
	
	public static String getBSONMatchQueryInOrdersColFromMongobench(Condition<ProductAttribute> condition, MutableBoolean refilterFlag) {	
		String res = null;	
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				ProductAttribute attr = ((SimpleCondition<ProductAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<ProductAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<ProductAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == ProductAttribute.id ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "asin': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "Orderline." + res;
					res = "'" + res;
					}
					if(attr == ProductAttribute.title ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "title': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "Orderline." + res;
					res = "'" + res;
					}
					if(attr == ProductAttribute.price ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "price': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "Orderline." + res;
					res = "'" + res;
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						res = "$expr: {$eq:[1,1]}";
					}
					
				}
			}
	
			if(condition instanceof AndCondition) {
				String bsonLeft = getBSONMatchQueryInOrdersColFromMongobench(((AndCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInOrdersColFromMongobench(((AndCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $and: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";
			}
	
			if(condition instanceof OrCondition) {
				String bsonLeft = getBSONMatchQueryInOrdersColFromMongobench(((OrCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInOrdersColFromMongobench(((OrCondition)condition).getRightCondition(), refilterFlag);			
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
	
	public Dataset<Product> getProductListInOrdersColFromMongobench(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = ProductServiceImpl.getBSONMatchQueryInOrdersColFromMongobench(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mongobench", "ordersCol", bsonQuery);
	
		Dataset<Product> res = dataset.flatMap((FlatMapFunction<Row, Product>) r -> {
				List<Product> list_res = new ArrayList<Product>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				Product product1 = new Product();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					array1 = r1.getAs("Orderline");
					if(array1!= null) {
						for (int i2 = 0; i2 < array1.size(); i2++){
							Row r2 = (Row) array1.apply(i2);
							Product product2 = (Product) product1.clone();
							boolean toAdd2  = false;
							WrappedArray array2  = null;
							// 	attribute Product.id for field asin			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("asin")) {
								if(nestedRow.getAs("asin")==null)
									product2.setId(null);
								else{
									product2.setId(Util.getStringValue(nestedRow.getAs("asin")));
									toAdd2 = true;					
									}
							}
							// 	attribute Product.title for field title			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("title")) {
								if(nestedRow.getAs("title")==null)
									product2.setTitle(null);
								else{
									product2.setTitle(Util.getStringValue(nestedRow.getAs("title")));
									toAdd2 = true;					
									}
							}
							// 	attribute Product.price for field price			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("price")) {
								if(nestedRow.getAs("price")==null)
									product2.setPrice(null);
								else{
									product2.setPrice(Util.getDoubleValue(nestedRow.getAs("price")));
									toAdd2 = true;					
									}
							}
							if(toAdd2&& (condition ==null || refilterFlag.booleanValue() || condition.evaluate(product2))) {
								list_res.add(product2);
								addedInList = true;
							} 
							if(addedInList)
								toAdd1 = false;
						}
					}
					
					if(toAdd1) {
						list_res.add(product1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(Product.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
		
	}
	
	public static Pair<String, List<String>> getSQLWhereClauseInProductTableFromMysqlbench(Condition<ProductAttribute> condition, MutableBoolean refilterFlag) {
		return getSQLWhereClauseInProductTableFromMysqlbenchWithTableAlias(condition, refilterFlag, "");
	}
	
	public static Pair<String, List<String>> getSQLWhereClauseInProductTableFromMysqlbenchWithTableAlias(Condition<ProductAttribute> condition, MutableBoolean refilterFlag, String tableAlias) {
		String where = null;	
		List<String> preparedValues = new java.util.ArrayList<String>();
		if(condition != null) {
			
			if(condition instanceof SimpleCondition) {
				ProductAttribute attr = ((SimpleCondition<ProductAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<ProductAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<ProductAttribute>) condition).getValue();
				if(value != null) {
					boolean isConditionAttrEncountered = false;
					if(attr == ProductAttribute.id ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "asin " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(attr == ProductAttribute.title ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "title " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(attr == ProductAttribute.price ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "price " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(attr == ProductAttribute.photo ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "imgUrl " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						where = "1 = 1";
					}
				}
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInProductTableFromMysqlbench(((AndCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInProductTableFromMysqlbench(((AndCondition) condition).getRightCondition(), refilterFlag);
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
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInProductTableFromMysqlbench(((OrCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInProductTableFromMysqlbench(((OrCondition) condition).getRightCondition(), refilterFlag);
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
	
	
	public Dataset<Product> getProductListInProductTableFromMysqlbench(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = ProductServiceImpl.getSQLWhereClauseInProductTableFromMysqlbench(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", "'" + Util.escapeQuote(preparedValue) + "'");
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("mysqlbench", "productTable", where);
		
	
		Dataset<Product> res = d.map((MapFunction<Row, Product>) r -> {
					Product product_res = new Product();
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
	
	
	
					return product_res;
				}, Encoders.bean(Product.class));
	
	
		return res;
		
	}
	
	
	// TODO get based on id(s). Ex:public Client getClientById(Long id)
	public Product getProductById(String id){
		Condition cond;
		cond = Condition.simple(ProductAttribute.id, conditions.Operator.EQUALS, id);
		Dataset<Product> res = getProductList(cond);
		if(res!=null)
			return res.first();
		return null;
	}
	
	public Dataset<Product> getProductListById(String id) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Product> getProductListByTitle(String title) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.title, conditions.Operator.EQUALS, title));
	}
	
	public Dataset<Product> getProductListByPrice(Double price) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.price, conditions.Operator.EQUALS, price));
	}
	
	public Dataset<Product> getProductListByPhoto(String photo) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.photo, conditions.Operator.EQUALS, photo));
	}
	
	
	
	
	public Dataset<Product> getOrderedProductsListInComposed_of(conditions.Condition<conditions.OrderAttribute> orderP_condition,conditions.Condition<conditions.ProductAttribute> orderedProducts_condition)		{
		MutableBoolean orderedProducts_refilter = new MutableBoolean(false);
		List<Dataset<Product>> datasetsPOJO = new ArrayList<Dataset<Product>>();
		Dataset<Order> all = null;
		boolean all_already_persisted = false;
		MutableBoolean orderP_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		
		Dataset<Composed_of> res_composed_of_orderedProducts;
		Dataset<Product> res_Product;
		// Role 'orderP' mapped to EmbeddedObject 'Orderline' 'Product' containing 'Order' 
		orderP_refilter = new MutableBoolean(false);
		res_composed_of_orderedProducts = composed_ofService.getComposed_ofListIndocSchemaordersColOrderline(orderP_condition, orderedProducts_condition, orderP_refilter, orderedProducts_refilter);
		if(orderP_refilter.booleanValue()) {
			if(all == null)
				all = new OrderServiceImpl().getOrderList(orderP_condition);
			joinCondition = null;
			joinCondition = res_composed_of_orderedProducts.col("orderP.id").equalTo(all.col("id"));
			if(joinCondition == null)
				res_Product = res_composed_of_orderedProducts.join(all).select("orderedProducts.*").as(Encoders.bean(Product.class));
			else
				res_Product = res_composed_of_orderedProducts.join(all, joinCondition).select("orderedProducts.*").as(Encoders.bean(Product.class));
		
		} else
			res_Product = res_composed_of_orderedProducts.map((MapFunction<Composed_of,Product>) r -> r.getOrderedProducts(), Encoders.bean(Product.class));
		res_Product = res_Product.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Product);
		
		
		//Join datasets or return 
		Dataset<Product> res = fullOuterJoinsProduct(datasetsPOJO);
		if(res == null)
			return null;
	
		List<Dataset<Product>> lonelyProductList = new ArrayList<Dataset<Product>>();
		lonelyProductList.add(getProductListInProductTableFromMysqlbench(orderedProducts_condition, new MutableBoolean(false)));
		Dataset<Product> lonelyProduct = fullOuterJoinsProduct(lonelyProductList);
		if(lonelyProduct != null) {
			res = fullLeftOuterJoinsProduct(Arrays.asList(res, lonelyProduct));
		}
		if(orderedProducts_refilter.booleanValue())
			res = res.filter((FilterFunction<Product>) r -> orderedProducts_condition == null || orderedProducts_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Product> getOrderedProductsListInComposed_ofByOrderPCondition(conditions.Condition<conditions.OrderAttribute> orderP_condition){
		return getOrderedProductsListInComposed_of(orderP_condition, null);
	}
	
	public Dataset<Product> getOrderedProductsListInComposed_ofByOrderP(pojo.Order orderP){
		if(orderP == null)
			return null;
	
		Condition c;
		c=Condition.simple(OrderAttribute.id,Operator.EQUALS, orderP.getId());
		Dataset<Product> res = getOrderedProductsListInComposed_ofByOrderPCondition(c);
		return res;
	}
	
	public Dataset<Product> getOrderedProductsListInComposed_ofByOrderedProductsCondition(conditions.Condition<conditions.ProductAttribute> orderedProducts_condition){
		return getOrderedProductsListInComposed_of(null, orderedProducts_condition);
	}
	public Dataset<Product> getReviewedProductListInHas_reviews(conditions.Condition<conditions.FeedbackAttribute> reviews_condition,conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition)		{
		MutableBoolean reviewedProduct_refilter = new MutableBoolean(false);
		List<Dataset<Product>> datasetsPOJO = new ArrayList<Dataset<Product>>();
		Dataset<Feedback> all = null;
		boolean all_already_persisted = false;
		MutableBoolean reviews_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		reviews_refilter = new MutableBoolean(false);
		// For role 'reviews' in reference 'product'  B->A Scenario
		Dataset<FeedbackTDO> feedbackTDOproductreviews = has_reviewsService.getFeedbackTDOListReviewsInProductInFeedbackFromKvSchema(reviews_condition, reviews_refilter);
		Dataset<ProductTDO> productTDOproductreviewedProduct = has_reviewsService.getProductTDOListReviewedProductInProductInFeedbackFromKvSchema(reviewedProduct_condition, reviewedProduct_refilter);
		if(reviews_refilter.booleanValue()) {
			if(all == null)
				all = new FeedbackServiceImpl().getFeedbackList(reviews_condition);
			joinCondition = null;
			if(joinCondition == null)
				feedbackTDOproductreviews = feedbackTDOproductreviews.as("A").join(all).select("A.*").as(Encoders.bean(FeedbackTDO.class));
			else
				feedbackTDOproductreviews = feedbackTDOproductreviews.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(FeedbackTDO.class));
		}
		Dataset<Row> res_product = 
			productTDOproductreviewedProduct.join(feedbackTDOproductreviews
				.withColumnRenamed("rate", "Feedback_rate")
				.withColumnRenamed("content", "Feedback_content")
				.withColumnRenamed("product", "Feedback_product")
				.withColumnRenamed("customer", "Feedback_customer")
				.withColumnRenamed("logEvents", "Feedback_logEvents"),
				productTDOproductreviewedProduct.col("kvSchema_feedback_product_asin").equalTo(feedbackTDOproductreviews.col("kvSchema_feedback_product_prodid")));
		Dataset<Product> res_Product_product = res_product.select( "id", "title", "price", "photo", "logEvents").as(Encoders.bean(Product.class));
		res_Product_product = res_Product_product.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Product_product);
		
		Dataset<Has_reviews> res_has_reviews_reviewedProduct;
		Dataset<Product> res_Product;
		
		
		//Join datasets or return 
		Dataset<Product> res = fullOuterJoinsProduct(datasetsPOJO);
		if(res == null)
			return null;
	
		List<Dataset<Product>> lonelyProductList = new ArrayList<Dataset<Product>>();
		lonelyProductList.add(getProductListInOrdersColFromMongobench(reviewedProduct_condition, new MutableBoolean(false)));
		Dataset<Product> lonelyProduct = fullOuterJoinsProduct(lonelyProductList);
		if(lonelyProduct != null) {
			res = fullLeftOuterJoinsProduct(Arrays.asList(res, lonelyProduct));
		}
		if(reviewedProduct_refilter.booleanValue())
			res = res.filter((FilterFunction<Product>) r -> reviewedProduct_condition == null || reviewedProduct_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Product> getReviewedProductListInHas_reviewsByReviewsCondition(conditions.Condition<conditions.FeedbackAttribute> reviews_condition){
		return getReviewedProductListInHas_reviews(reviews_condition, null);
	}
	
	public Dataset<Product> getReviewedProductListInHas_reviewsByReviews(pojo.Feedback reviews){
		if(reviews == null)
			return null;
	
		Condition c;
		c=null;
		Dataset<Product> res = getReviewedProductListInHas_reviewsByReviewsCondition(c);
		return res;
	}
	
	public Dataset<Product> getReviewedProductListInHas_reviewsByReviewedProductCondition(conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition){
		return getReviewedProductListInHas_reviews(null, reviewedProduct_condition);
	}
	
	
	public boolean insertProduct(Product product){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertProductInProductTableFromMysqlbench(product) || inserted ;
		return inserted;
	}
	
	public boolean insertProductInProductTableFromMysqlbench(Product product)	{
		Condition<ProductAttribute> conditionID;
		String idvalue="";
		boolean entityExists=false;
		conditionID = Condition.simple(ProductAttribute.id, Operator.EQUALS, product.getId());
		idvalue+=product.getId();
		Dataset res = getProductListInProductTableFromMysqlbench(conditionID,new MutableBoolean(false));
		entityExists = res != null && !res.isEmpty();
				
		if(!entityExists){
		List<String> columns = new ArrayList<>();
		List<Object> values = new ArrayList<>();	
		columns.add("asin");
		values.add(product.getId());
		columns.add("title");
		values.add(product.getTitle());
		columns.add("price");
		values.add(product.getPrice());
		columns.add("imgUrl");
		values.add(product.getPhoto());
		DBConnectionMgr.insertInTable(columns, Arrays.asList(values), "productTable", "mysqlbench");
			logger.info("Inserted [Product] entity ID [{}] in [ProductTable] in database [Mysqlbench]", idvalue);
		}
		else
			logger.warn("[Product] entity ID [{}] already present in [ProductTable] in database [Mysqlbench]", idvalue);
		return !entityExists;
	} 
	
	public void updateProductList(conditions.Condition<conditions.ProductAttribute> condition, conditions.SetClause<conditions.ProductAttribute> set){
		//TODO
	}
	
	public void updateProduct(pojo.Product product) {
		//TODO using the id
		return;
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
	public void updateReviewedProductListInHas_reviews(
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition,
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,
		
		conditions.SetClause<conditions.ProductAttribute> set
	){
		//TODO
	}
	
	public void updateReviewedProductListInHas_reviewsByReviewsCondition(
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateReviewedProductListInHas_reviews(reviews_condition, null, set);
	}
	
	public void updateReviewedProductListInHas_reviewsByReviews(
		pojo.Feedback reviews,
		conditions.SetClause<conditions.ProductAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateReviewedProductListInHas_reviewsByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateReviewedProductListInHas_reviews(null, reviewedProduct_condition, set);
	}
	
	
	public void deleteProductList(conditions.Condition<conditions.ProductAttribute> condition){
		//TODO
	}
	
	public void deleteProduct(pojo.Product product) {
		//TODO using the id
		return;
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
	public void deleteReviewedProductListInHas_reviews(	
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition,	
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition){
			//TODO
		}
	
	public void deleteReviewedProductListInHas_reviewsByReviewsCondition(
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition
	){
		deleteReviewedProductListInHas_reviews(reviews_condition, null);
	}
	
	public void deleteReviewedProductListInHas_reviewsByReviews(
		pojo.Feedback reviews 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteReviewedProductListInHas_reviewsByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition
	){
		deleteReviewedProductListInHas_reviews(null, reviewedProduct_condition);
	}
	
}
