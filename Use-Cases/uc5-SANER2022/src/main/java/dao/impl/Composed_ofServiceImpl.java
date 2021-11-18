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
import conditions.Composed_ofAttribute;
import conditions.Operator;
import pojo.Composed_of;
import tdo.OrderTDO;
import tdo.Composed_ofTDO;
import pojo.Order;
import conditions.OrderAttribute;
import dao.services.OrderService;
import tdo.ProductTDO;
import tdo.Composed_ofTDO;
import pojo.Product;
import conditions.ProductAttribute;
import dao.services.ProductService;
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

public class Composed_ofServiceImpl extends dao.services.Composed_ofService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Composed_ofServiceImpl.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	//join structure
	// Left side 'orderid' of reference [orderRef ]
	public Dataset<OrderTDO> getOrderTDOListOrderPInOrderRefInOrderTableFromRelSchemaB(Condition<OrderAttribute> condition, MutableBoolean refilterFlag){	
	
		Pair<String, List<String>> whereClause = OrderServiceImpl.getSQLWhereClauseInOrderTableFromMysqlModelB(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", "'" + Util.escapeQuote(preparedValue) + "'");
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("mysqlModelB", "orderTable", where);
		
	
		Dataset<OrderTDO> res = d.map((MapFunction<Row, OrderTDO>) r -> {
					OrderTDO order_res = new OrderTDO();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Order.Id]
					String id = Util.getStringValue(r.getAs("orderId"));
					order_res.setId(id);
					
					// attribute [Order.Orderdate]
					LocalDate orderdate = Util.getLocalDateValue(r.getAs("orderDate"));
					order_res.setOrderdate(orderdate);
	
					// Get reference column [orderId ] for reference [orderRef]
					String mongoSchemaB_detailOrderCol_orderRef_orderId = r.getAs("orderId") == null ? null : r.getAs("orderId").toString();
					order_res.setMongoSchemaB_detailOrderCol_orderRef_orderId(mongoSchemaB_detailOrderCol_orderRef_orderId);
	
	
					return order_res;
				}, Encoders.bean(OrderTDO.class));
	
	
		return res;
	}
	// return join values orderRef productRef
	public Dataset<Composed_ofTDO> getComposed_ofTDOListInComposed_of_OrderRef_ProductRefInDetailOrderColFromMongoSchemaB(){	
		Condition condition = null;
		MutableBoolean refilterFlag = new MutableBoolean(false);
		String bsonQuery = Composed_ofServiceImpl.getBSONMatchQueryInDetailOrderColFromMongoModelB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mongoModelB", "detailOrderCol", bsonQuery);
	
		Dataset<Composed_ofTDO> res = dataset.flatMap((FlatMapFunction<Row, Composed_ofTDO>) r -> {
				List<Composed_ofTDO> list_res = new ArrayList<Composed_ofTDO>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				Composed_ofTDO composed_of1 = new Composed_ofTDO();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					
						// field  orderid for reference orderRef . Reference field : orderid
					nestedRow =  r1;
					if(nestedRow != null) {
						composed_of1.setMongoSchemaB_detailOrderCol_orderRef_orderid(nestedRow.getAs("orderid") == null ? null : nestedRow.getAs("orderid").toString());
						toAdd1 = true;					
					}
						// field  productid for reference productRef . Reference field : productid
					nestedRow =  r1;
					if(nestedRow != null) {
						composed_of1.setMongoSchemaB_detailOrderCol_productRef_productid(nestedRow.getAs("productid") == null ? null : nestedRow.getAs("productid").toString());
						toAdd1 = true;					
					}
					
					
					if(toAdd1) {
						list_res.add(composed_of1);
						addedInList = true;
					} 
					if(toAdd1) {
						list_res.add(composed_of1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(Composed_ofTDO.class));
		res= res.dropDuplicates(new String[]{});
		return res;
	}
	
	public static String getBSONMatchQueryInDetailOrderColFromMongoModelB(Condition<Composed_ofAttribute> condition, MutableBoolean refilterFlag) {	
		String res = null;	
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				Composed_ofAttribute attr = ((SimpleCondition<Composed_ofAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<Composed_ofAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<Composed_ofAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						res = "$expr: {$eq:[1,1]}";
					}
					
				}
			}
	
			if(condition instanceof AndCondition) {
				String bsonLeft = getBSONMatchQueryInDetailOrderColFromMongoModelB(((AndCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInDetailOrderColFromMongoModelB(((AndCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $and: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";
			}
	
			if(condition instanceof OrCondition) {
				String bsonLeft = getBSONMatchQueryInDetailOrderColFromMongoModelB(((OrCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInDetailOrderColFromMongoModelB(((OrCondition)condition).getRightCondition(), refilterFlag);			
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
	
	
	
	//join structure
	// Left side 'productid' of reference [productRef ]
	public Dataset<ProductTDO> getProductTDOListOrderedProductsInProductRefInProductsFromKvSchemaB(Condition<ProductAttribute> condition, MutableBoolean refilterFlag){	
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
		Dataset<ProductTDO> res = rows.map((MapFunction<Row, ProductTDO>) r -> {
					ProductTDO product_res = new ProductTDO();
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
					//Checking that reference field 'asin' is mapped in Key
					if(fieldsListInKey.contains("asin")){
						//Retrieving reference field 'asin' in Key
						Pattern pattern_asin = Pattern.compile("\\*");
				        Matcher match_asin = pattern_asin.matcher(finalKeypattern);
						regex = finalKeypattern.replaceAll("\\*","(.*)");
						groupindex = fieldsListInKey.indexOf("asin")+1;
						if(groupindex==null) {
							logger.warn("Attribute 'Product' mapped physical field 'asin' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
						}
						p = Pattern.compile(regex);
						m = p.matcher(key);
						matches = m.find();
						String productRef_asin = null;
						if(matches) {
						productRef_asin = m.group(groupindex.intValue());
						} else {
						logger.warn("Cannot retrieve value 'asin'. Probably due to an ambiguous regex.");
						product_res.addLogEvent("Cannot retrieve value for 'asin' attribute stored in db redisModelB. Probably due to an ambiguous regex.");
						}
						product_res.setMongoSchemaB_detailOrderCol_productRef_asin(productRef_asin);
					}
	
						return product_res;
				}, Encoders.bean(ProductTDO.class));
		if(refilterFlag.booleanValue())
			res = res.filter((FilterFunction<ProductTDO>) r -> condition == null || condition.evaluate(r));
		res=res.dropDuplicates(new String[] {"id"});
		return res;
	}
	// return join values productRef orderRef
	public Dataset<Composed_ofTDO> getComposed_ofTDOListInComposed_of_ProductRef_OrderRefInDetailOrderColFromMongoSchemaB(){	
		Condition condition = null;
		MutableBoolean refilterFlag = new MutableBoolean(false);
		String bsonQuery = Composed_ofServiceImpl.getBSONMatchQueryInDetailOrderColFromMongoModelB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mongoModelB", "detailOrderCol", bsonQuery);
	
		Dataset<Composed_ofTDO> res = dataset.flatMap((FlatMapFunction<Row, Composed_ofTDO>) r -> {
				List<Composed_ofTDO> list_res = new ArrayList<Composed_ofTDO>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				Composed_ofTDO composed_of1 = new Composed_ofTDO();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					
						// field  productid for reference productRef . Reference field : productid
					nestedRow =  r1;
					if(nestedRow != null) {
						composed_of1.setMongoSchemaB_detailOrderCol_productRef_productid(nestedRow.getAs("productid") == null ? null : nestedRow.getAs("productid").toString());
						toAdd1 = true;					
					}
						// field  orderid for reference orderRef . Reference field : orderid
					nestedRow =  r1;
					if(nestedRow != null) {
						composed_of1.setMongoSchemaB_detailOrderCol_orderRef_orderid(nestedRow.getAs("orderid") == null ? null : nestedRow.getAs("orderid").toString());
						toAdd1 = true;					
					}
					
					
					if(toAdd1) {
						list_res.add(composed_of1);
						addedInList = true;
					} 
					if(toAdd1) {
						list_res.add(composed_of1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(Composed_ofTDO.class));
		res= res.dropDuplicates(new String[]{});
		return res;
	}
	
	
	
	
	
	
	
	
	public Dataset<pojo.Composed_of> getComposed_ofList(
		conditions.Condition<conditions.OrderAttribute> orderP_condition,
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition){
			Composed_ofServiceImpl composed_ofService = this;
			OrderService orderService = new OrderServiceImpl();  
			ProductService productService = new ProductServiceImpl();
			MutableBoolean orderP_refilter = new MutableBoolean(false);
			List<Dataset<Composed_of>> datasetsPOJO = new ArrayList<Dataset<Composed_of>>();
			boolean all_already_persisted = false;
			MutableBoolean orderedProducts_refilter = new MutableBoolean(false);
			org.apache.spark.sql.Column joinCondition = null;
			// join physical structure A<-AB->B
			// (A) (AB) (B)  OR (A B) (AB) Join table is 'alone'
			orderedProducts_refilter = new MutableBoolean(false);
			Dataset<OrderTDO> orderTDOorderReforderP = this.getOrderTDOListOrderPInOrderRefInOrderTableFromRelSchemaB(orderP_condition, orderP_refilter);
			Dataset<Composed_ofTDO> composed_ofTDOorderRef_productRef = this.getComposed_ofTDOListInComposed_of_OrderRef_ProductRefInDetailOrderColFromMongoSchemaB();
			Dataset<ProductTDO> productTDOorderReforderedProducts = this.getProductTDOListOrderedProductsInProductRefInProductsFromKvSchemaB(orderedProducts_condition, orderedProducts_refilter);
			
			Dataset<Row> A_orderTDOorderReforderP  = orderTDOorderReforderP
				.withColumnRenamed("id", "A_id")
				.withColumnRenamed("orderdate", "A_orderdate")
				.withColumnRenamed("totalprice", "A_totalprice")
				.withColumnRenamed("logEvents", "A_logEvents");
			
			Dataset<Row> C_productTDOorderReforderedProducts = productTDOorderReforderedProducts
				.withColumnRenamed("id", "C_id")
				.withColumnRenamed("title", "C_title")
				.withColumnRenamed("price", "C_price")
				.withColumnRenamed("photo", "C_photo")
				.withColumnRenamed("logEvents", "C_logEvents");
			
			Dataset<Row> res_orderRef = A_orderTDOorderReforderP.join(composed_ofTDOorderRef_productRef,
											A_orderTDOorderReforderP.col("mongoSchemaB_detailOrderCol_orderRef_orderId").equalTo(composed_ofTDOorderRef_productRef.col("mongoSchemaB_detailOrderCol_orderRef_orderid")));
			res_orderRef = res_orderRef.join(C_productTDOorderReforderedProducts,
				res_orderRef.col("mongoSchemaB_detailOrderCol_productRef_productid").equalTo(C_productTDOorderReforderedProducts.col("mongoSchemaB_detailOrderCol_productRef_asin")));
			
			Dataset<Composed_of> res_Order_orderRef = res_orderRef.map(new MapFunction<Row, Composed_of>() {
						@Override
						public Composed_of call(Row r) throws Exception {
							Composed_of res = new Composed_of();
							Order A = new Order();
							Product C = new Product();
							A.setId(r.getAs("A_id"));
							A.setOrderdate(r.getAs("A_orderdate"));
							A.setTotalprice(r.getAs("A_totalprice"));
							A.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("A_logEvents")));
			
							res.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("logEvents")));				
			
							C.setId(r.getAs("C_id"));
							C.setTitle(r.getAs("C_title"));
							C.setPrice(r.getAs("C_price"));
							C.setPhoto(r.getAs("C_photo"));
							C.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("C_logEvents")));
							
							res.setOrderP(A);
							res.setOrderedProducts(C);
							return res;
						}
					}, Encoders.bean(Composed_of.class));
			
			datasetsPOJO.add(res_Order_orderRef);	
			
		
			
			Dataset<Composed_of> res_composed_of_orderP;
			Dataset<Order> res_Order;
			
			
			//Join datasets or return 
			Dataset<Composed_of> res = fullOuterJoinsComposed_of(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Order> lonelyOrderP = null;
			Dataset<Product> lonelyOrderedProducts = null;
			
			List<Dataset<Order>> lonelyorderPList = new ArrayList<Dataset<Order>>();
			lonelyorderPList.add(orderService.getOrderListInUserColFromMongoModelB(orderP_condition, new MutableBoolean(false)));
			lonelyOrderP = OrderService.fullOuterJoinsOrder(lonelyorderPList);
			if(lonelyOrderP != null) {
				res = fullLeftOuterJoinBetweenComposed_ofAndOrderP(res, lonelyOrderP);
			}	
		
		
			
			if(orderP_refilter.booleanValue() || orderedProducts_refilter.booleanValue())
				res = res.filter((FilterFunction<Composed_of>) r -> (orderP_condition == null || orderP_condition.evaluate(r.getOrderP())) && (orderedProducts_condition == null || orderedProducts_condition.evaluate(r.getOrderedProducts())));
			
		
			return res;
		
		}
	
	public Dataset<pojo.Composed_of> getComposed_ofListByOrderPCondition(
		conditions.Condition<conditions.OrderAttribute> orderP_condition
	){
		return getComposed_ofList(orderP_condition, null);
	}
	
	public Dataset<pojo.Composed_of> getComposed_ofListByOrderP(pojo.Order orderP) {
		conditions.Condition<conditions.OrderAttribute> cond = null;
		cond = conditions.Condition.simple(conditions.OrderAttribute.id, conditions.Operator.EQUALS, orderP.getId());
		Dataset<pojo.Composed_of> res = getComposed_ofListByOrderPCondition(cond);
	return res;
	}
	public Dataset<pojo.Composed_of> getComposed_ofListByOrderedProductsCondition(
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition
	){
		return getComposed_ofList(null, orderedProducts_condition);
	}
	
	public Dataset<pojo.Composed_of> getComposed_ofListByOrderedProducts(pojo.Product orderedProducts) {
		conditions.Condition<conditions.ProductAttribute> cond = null;
		cond = conditions.Condition.simple(conditions.ProductAttribute.id, conditions.Operator.EQUALS, orderedProducts.getId());
		Dataset<pojo.Composed_of> res = getComposed_ofListByOrderedProductsCondition(cond);
	return res;
	}
	
	public void insertComposed_of(Composed_of composed_of){
		//Link entities in join structures.
		insertComposed_ofInJoinStructDetailOrderColInMongoModelB(composed_of);
		// Update embedded structures mapped to non mandatory roles.
		// Update ref fields mapped to non mandatory roles. 
	}
	
	public 	boolean insertComposed_ofInJoinStructDetailOrderColInMongoModelB(Composed_of composed_of){
	 	// Rel 'composed_of' Insert in join structure 'detailOrderCol'
		List<String> columns = new ArrayList<>();
		List<Object> values = new ArrayList<>();
		List<List<Object>> rows = new ArrayList<>();
		Order order = composed_of.getOrderP();
		Product product = composed_of.getOrderedProducts();
		// Role in join structure 
		columns.add("orderid");
		Object orderId = order.getId();
		values.add(orderId);
		// Role in join structure 
		columns.add("productid");
		Object productId = product.getId();
		values.add(productId);
		rows.add(values);
		DBConnectionMgr.insertInTable(columns, rows, "detailOrderCol", "mongoModelB"); 					
		return true;
	}
	
	
	
	
	
	
	public void deleteComposed_ofList(
		conditions.Condition<conditions.OrderAttribute> orderP_condition,
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition){
			//TODO
		}
	
	public void deleteComposed_ofListByOrderPCondition(
		conditions.Condition<conditions.OrderAttribute> orderP_condition
	){
		deleteComposed_ofList(orderP_condition, null);
	}
	
	public void deleteComposed_ofListByOrderP(pojo.Order orderP) {
		// TODO using id for selecting
		return;
	}
	public void deleteComposed_ofListByOrderedProductsCondition(
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition
	){
		deleteComposed_ofList(null, orderedProducts_condition);
	}
	
	public void deleteComposed_ofListByOrderedProducts(pojo.Product orderedProducts) {
		// TODO using id for selecting
		return;
	}
		
}
