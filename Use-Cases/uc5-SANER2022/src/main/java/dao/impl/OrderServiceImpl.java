package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Order;
import conditions.*;
import dao.services.OrderService;
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


public class OrderServiceImpl extends OrderService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OrderServiceImpl.class);
	
	
	
	
	public static Pair<String, List<String>> getSQLWhereClauseInOrderTableFromMysqlModelB(Condition<OrderAttribute> condition, MutableBoolean refilterFlag) {
		return getSQLWhereClauseInOrderTableFromMysqlModelBWithTableAlias(condition, refilterFlag, "");
	}
	
	public static Pair<String, List<String>> getSQLWhereClauseInOrderTableFromMysqlModelBWithTableAlias(Condition<OrderAttribute> condition, MutableBoolean refilterFlag, String tableAlias) {
		String where = null;	
		List<String> preparedValues = new java.util.ArrayList<String>();
		if(condition != null) {
			
			if(condition instanceof SimpleCondition) {
				OrderAttribute attr = ((SimpleCondition<OrderAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<OrderAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<OrderAttribute>) condition).getValue();
				if(value != null) {
					boolean isConditionAttrEncountered = false;
					if(attr == OrderAttribute.id ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "orderId " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(attr == OrderAttribute.orderdate ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "orderDate " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						where = "1 = 1";
					}
				}
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInOrderTableFromMysqlModelB(((AndCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInOrderTableFromMysqlModelB(((AndCondition) condition).getRightCondition(), refilterFlag);
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
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInOrderTableFromMysqlModelB(((OrCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInOrderTableFromMysqlModelB(((OrCondition) condition).getRightCondition(), refilterFlag);
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
	
	
	public Dataset<Order> getOrderListInOrderTableFromMysqlModelB(conditions.Condition<conditions.OrderAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = OrderServiceImpl.getSQLWhereClauseInOrderTableFromMysqlModelB(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", "'" + Util.escapeQuote(preparedValue) + "'");
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("mysqlModelB", "orderTable", where);
		
	
		Dataset<Order> res = d.map((MapFunction<Row, Order>) r -> {
					Order order_res = new Order();
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
	
	
	
					return order_res;
				}, Encoders.bean(Order.class));
	
	
		return res;
		
	}
	
	
	public static String getBSONMatchQueryInUserColFromMongoModelB(Condition<OrderAttribute> condition, MutableBoolean refilterFlag) {	
		String res = null;	
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				OrderAttribute attr = ((SimpleCondition<OrderAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<OrderAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<OrderAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == OrderAttribute.id ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "id': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "orders." + res;
					res = "'" + res;
					}
					if(attr == OrderAttribute.orderdate ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "buydate': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "orders." + res;
					res = "'" + res;
					}
					if(attr == OrderAttribute.totalprice ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "totalamount': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "orders." + res;
					res = "'" + res;
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						res = "$expr: {$eq:[1,1]}";
					}
					
				}
			}
	
			if(condition instanceof AndCondition) {
				String bsonLeft = getBSONMatchQueryInUserColFromMongoModelB(((AndCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInUserColFromMongoModelB(((AndCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $and: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";
			}
	
			if(condition instanceof OrCondition) {
				String bsonLeft = getBSONMatchQueryInUserColFromMongoModelB(((OrCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInUserColFromMongoModelB(((OrCondition)condition).getRightCondition(), refilterFlag);			
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
	
	public Dataset<Order> getOrderListInUserColFromMongoModelB(conditions.Condition<conditions.OrderAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = OrderServiceImpl.getBSONMatchQueryInUserColFromMongoModelB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mongoModelB", "userCol", bsonQuery);
	
		Dataset<Order> res = dataset.flatMap((FlatMapFunction<Row, Order>) r -> {
				List<Order> list_res = new ArrayList<Order>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				Order order1 = new Order();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					array1 = r1.getAs("orders");
					if(array1!= null) {
						for (int i2 = 0; i2 < array1.size(); i2++){
							Row r2 = (Row) array1.apply(i2);
							Order order2 = (Order) order1.clone();
							boolean toAdd2  = false;
							WrappedArray array2  = null;
							// 	attribute Order.id for field id			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("id")) {
								if(nestedRow.getAs("id")==null)
									order2.setId(null);
								else{
									order2.setId(Util.getStringValue(nestedRow.getAs("id")));
									toAdd2 = true;					
									}
							}
							// 	attribute Order.orderdate for field buydate			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("buydate")) {
								if(nestedRow.getAs("buydate")==null)
									order2.setOrderdate(null);
								else{
									order2.setOrderdate(Util.getLocalDateValue(nestedRow.getAs("buydate")));
									toAdd2 = true;					
									}
							}
							// 	attribute Order.totalprice for field totalamount			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("totalamount")) {
								if(nestedRow.getAs("totalamount")==null)
									order2.setTotalprice(null);
								else{
									order2.setTotalprice(Util.getDoubleValue(nestedRow.getAs("totalamount")));
									toAdd2 = true;					
									}
							}
							if(toAdd2&& (condition ==null || refilterFlag.booleanValue() || condition.evaluate(order2))) {
								list_res.add(order2);
								addedInList = true;
							} 
							if(addedInList)
								toAdd1 = false;
						}
					}
					
					if(toAdd1) {
						list_res.add(order1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(Order.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Order> getOrderListInBuys(conditions.Condition<conditions.OrderAttribute> order_condition,conditions.Condition<conditions.CustomerAttribute> client_condition)		{
		MutableBoolean order_refilter = new MutableBoolean(false);
		List<Dataset<Order>> datasetsPOJO = new ArrayList<Dataset<Order>>();
		Dataset<Customer> all = null;
		boolean all_already_persisted = false;
		MutableBoolean client_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// For role 'order' in reference 'clientRef'. A->B Scenario
		client_refilter = new MutableBoolean(false);
		Dataset<OrderTDO> orderTDOclientReforder = buysService.getOrderTDOListOrderInClientRefInOrderTableFromRelSchemaB(order_condition, order_refilter);
		Dataset<CustomerTDO> customerTDOclientRefclient = buysService.getCustomerTDOListClientInClientRefInOrderTableFromRelSchemaB(client_condition, client_refilter);
		if(client_refilter.booleanValue()) {
			if(all == null)
				all = new CustomerServiceImpl().getCustomerList(client_condition);
			joinCondition = null;
			joinCondition = customerTDOclientRefclient.col("id").equalTo(all.col("id"));
			if(joinCondition == null)
				customerTDOclientRefclient = customerTDOclientRefclient.as("A").join(all).select("A.*").as(Encoders.bean(CustomerTDO.class));
			else
				customerTDOclientRefclient = customerTDOclientRefclient.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(CustomerTDO.class));
		}
	
		
		Dataset<Row> res_clientRef = orderTDOclientReforder.join(customerTDOclientRefclient
				.withColumnRenamed("id", "Customer_id")
				.withColumnRenamed("firstname", "Customer_firstname")
				.withColumnRenamed("lastname", "Customer_lastname")
				.withColumnRenamed("gender", "Customer_gender")
				.withColumnRenamed("birthday", "Customer_birthday")
				.withColumnRenamed("creationDate", "Customer_creationDate")
				.withColumnRenamed("locationip", "Customer_locationip")
				.withColumnRenamed("browser", "Customer_browser")
				.withColumnRenamed("logEvents", "Customer_logEvents"),
				orderTDOclientReforder.col("relSchemaB_orderTable_clientRef_customerId").equalTo(customerTDOclientRefclient.col("relSchemaB_orderTable_clientRef_id")));
		Dataset<Order> res_Order_clientRef = res_clientRef.select( "id", "orderdate", "totalprice", "logEvents").as(Encoders.bean(Order.class));
		
		res_Order_clientRef = res_Order_clientRef.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Order_clientRef);
		
		
		Dataset<Buys> res_buys_order;
		Dataset<Order> res_Order;
		// Role 'client' mapped to EmbeddedObject 'orders' 'Order' containing 'Customer' 
		client_refilter = new MutableBoolean(false);
		res_buys_order = buysService.getBuysListInmongoSchemaBuserColorders(client_condition, order_condition, client_refilter, order_refilter);
		if(client_refilter.booleanValue()) {
			if(all == null)
				all = new CustomerServiceImpl().getCustomerList(client_condition);
			joinCondition = null;
			joinCondition = res_buys_order.col("client.id").equalTo(all.col("id"));
			if(joinCondition == null)
				res_Order = res_buys_order.join(all).select("order.*").as(Encoders.bean(Order.class));
			else
				res_Order = res_buys_order.join(all, joinCondition).select("order.*").as(Encoders.bean(Order.class));
		
		} else
			res_Order = res_buys_order.map((MapFunction<Buys,Order>) r -> r.getOrder(), Encoders.bean(Order.class));
		res_Order = res_Order.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Order);
		
		
		//Join datasets or return 
		Dataset<Order> res = fullOuterJoinsOrder(datasetsPOJO);
		if(res == null)
			return null;
	
		if(order_refilter.booleanValue())
			res = res.filter((FilterFunction<Order>) r -> order_condition == null || order_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Order> getOrderPListInComposed_of(conditions.Condition<conditions.OrderAttribute> orderP_condition,conditions.Condition<conditions.ProductAttribute> orderedProducts_condition)		{
		MutableBoolean orderP_refilter = new MutableBoolean(false);
		List<Dataset<Order>> datasetsPOJO = new ArrayList<Dataset<Order>>();
		Dataset<Product> all = null;
		boolean all_already_persisted = false;
		MutableBoolean orderedProducts_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// join physical structure A<-AB->B
		// (A) (AB) (B)  OR (A B) (AB) Join table is 'alone'
		orderedProducts_refilter = new MutableBoolean(false);
		Dataset<OrderTDO> orderTDOorderReforderP = composed_ofService.getOrderTDOListOrderPInOrderRefInOrderTableFromRelSchemaB(orderP_condition, orderP_refilter);
		Dataset<Composed_ofTDO> composed_ofTDOorderRef_productRef = composed_ofService.getComposed_ofTDOListInComposed_of_OrderRef_ProductRefInDetailOrderColFromMongoSchemaB();
		Dataset<ProductTDO> productTDOorderReforderedProducts = composed_ofService.getProductTDOListOrderedProductsInProductRefInProductsFromKvSchemaB(orderedProducts_condition, orderedProducts_refilter);
		if(orderedProducts_refilter.booleanValue()) {
			if(all == null)
					all = new ProductServiceImpl().getProductList(orderedProducts_condition);
			joinCondition = null;
				joinCondition = productTDOorderReforderedProducts.col("id").equalTo(all.col("id"));
				productTDOorderReforderedProducts = productTDOorderReforderedProducts.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(ProductTDO.class));
		}
		Dataset<Row> res_orderRef = orderTDOorderReforderP.join(composed_ofTDOorderRef_productRef.withColumnRenamed("logEvents", "composed_of_logEvents"),
										orderTDOorderReforderP.col("mongoSchemaB_detailOrderCol_orderRef_orderId").equalTo(composed_ofTDOorderRef_productRef.col("mongoSchemaB_detailOrderCol_orderRef_orderid")));
		res_orderRef = res_orderRef.join(productTDOorderReforderedProducts
			.withColumnRenamed("id", "Product_id")
			.withColumnRenamed("title", "Product_title")
			.withColumnRenamed("price", "Product_price")
			.withColumnRenamed("photo", "Product_photo")
			.withColumnRenamed("logEvents", "Product_logEvents"),
			res_orderRef.col("mongoSchemaB_detailOrderCol_productRef_productid").equalTo(productTDOorderReforderedProducts.col("mongoSchemaB_detailOrderCol_productRef_asin")));
		Dataset<Order> res_Order_orderRef = res_orderRef.select( "id", "orderdate", "totalprice", "logEvents").as(Encoders.bean(Order.class));
		datasetsPOJO.add(res_Order_orderRef.dropDuplicates(new String[] {"id"}));	
		
		
		
		Dataset<Composed_of> res_composed_of_orderP;
		Dataset<Order> res_Order;
		
		
		//Join datasets or return 
		Dataset<Order> res = fullOuterJoinsOrder(datasetsPOJO);
		if(res == null)
			return null;
	
		List<Dataset<Order>> lonelyOrderList = new ArrayList<Dataset<Order>>();
		lonelyOrderList.add(getOrderListInUserColFromMongoModelB(orderP_condition, new MutableBoolean(false)));
		Dataset<Order> lonelyOrder = fullOuterJoinsOrder(lonelyOrderList);
		if(lonelyOrder != null) {
			res = fullLeftOuterJoinsOrder(Arrays.asList(res, lonelyOrder));
		}
		if(orderP_refilter.booleanValue())
			res = res.filter((FilterFunction<Order>) r -> orderP_condition == null || orderP_condition.evaluate(r));
		
	
		return res;
		}
	
	public boolean insertOrder(
		Order order,
		Customer	clientBuys,
		 List<Product> orderedProductsComposed_of){
		 	boolean inserted = false;
		 	// Insert in standalone structures
		 	// Insert in structures containing double embedded role
		 	// Insert in descending structures
		 	// Insert in ascending structures 
		 	inserted = insertOrderInUserColFromMongoModelB(order,clientBuys,orderedProductsComposed_of)|| inserted ;
		 	// Insert in ref structures 
		 	inserted = insertOrderInOrderTableFromMysqlModelB(order,clientBuys,orderedProductsComposed_of)|| inserted ;
		 	inserted = insertOrderInDetailOrderColFromMongoModelB(order,clientBuys,orderedProductsComposed_of)|| inserted ;
		 	// Insert in ref structures mapped to opposite role of mandatory role  
		 	return inserted;
		 }
	
	public boolean insertOrderInUserColFromMongoModelB(Order order,
		Customer	clientBuys,
		 List<Product> orderedProductsComposed_of)	{
			 // Implement Insert in ascending complex struct
		
			Bson filter= new Document();
			Bson updateOp;
			String addToSet;
			List<String> fieldName= new ArrayList();
			List<Bson> arrayFilterCond = new ArrayList();
			Document docorders_1 = new Document();
			docorders_1.append("id",order.getId());
			docorders_1.append("buydate",order.getOrderdate());
			docorders_1.append("totalamount",order.getTotalprice());
			
			// level 1 ascending
			Customer customer = clientBuys;
				filter = eq("id",customer.getId());
				updateOp = addToSet("orders", docorders_1);
				DBConnectionMgr.upsertMany(filter, updateOp, "userCol", "mongoModelB");					
		
			return true;
		}
	public boolean insertOrderInOrderTableFromMysqlModelB(Order order,
		Customer	clientBuys,
		 List<Product> orderedProductsComposed_of)	{
			 // Implement Insert in structures with mandatory references
			List<String> columns = new ArrayList<>();
			List<Object> values = new ArrayList<>();
			List<List<Object>> rows = new ArrayList<>();
		columns.add("orderId");
		values.add(order.getId());
		columns.add("orderDate");
		values.add(order.getOrderdate());
			// Ref 'clientRef' mapped to role 'order'
			columns.add("customerId");
			values.add(clientBuys.getId());
			rows.add(values);
			DBConnectionMgr.insertInTable(columns, rows, "orderTable", "mysqlModelB");
			return true;
		}
	public boolean insertOrderInDetailOrderColFromMongoModelB(Order order,
		Customer	clientBuys,
		 List<Product> orderedProductsComposed_of)	{
			 // Implement Insert in structures with mandatory references
			// In insertRefStruct in MongoDB
			Bson filter = new Document();
			Bson updateOp;
			List<Document> docsList = new ArrayList();
			// Role in join structure 
			String roleEntityField = "orderid";
			for(Product product : orderedProductsComposed_of){
				Document doc = new Document();
				doc.append(roleEntityField,order.getId());
				doc.append("productid",product.getId());
				docsList.add(doc);
			}	
			DBConnectionMgr.insertMany(docsList, "detailOrderCol", "mongoModelB");
			return true;
		}
	public void updateOrderList(conditions.Condition<conditions.OrderAttribute> condition, conditions.SetClause<conditions.OrderAttribute> set){
		//TODO
	}
	
	public void updateOrder(pojo.Order order) {
		//TODO using the id
		return;
	}
	public void updateOrderListInBuys(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.CustomerAttribute> client_condition,
		
		conditions.SetClause<conditions.OrderAttribute> set
	){
		//TODO
	}
	
	public void updateOrderListInBuysByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInBuys(order_condition, null, set);
	}
	public void updateOrderListInBuysByClientCondition(
		conditions.Condition<conditions.CustomerAttribute> client_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInBuys(null, client_condition, set);
	}
	
	public void updateOrderListInBuysByClient(
		pojo.Customer client,
		conditions.SetClause<conditions.OrderAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateOrderPListInComposed_of(
		conditions.Condition<conditions.OrderAttribute> orderP_condition,
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition,
		
		conditions.SetClause<conditions.OrderAttribute> set
	){
		//TODO
	}
	
	public void updateOrderPListInComposed_ofByOrderPCondition(
		conditions.Condition<conditions.OrderAttribute> orderP_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderPListInComposed_of(orderP_condition, null, set);
	}
	public void updateOrderPListInComposed_ofByOrderedProductsCondition(
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderPListInComposed_of(null, orderedProducts_condition, set);
	}
	
	public void updateOrderPListInComposed_ofByOrderedProducts(
		pojo.Product orderedProducts,
		conditions.SetClause<conditions.OrderAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	
	
	public void deleteOrderList(conditions.Condition<conditions.OrderAttribute> condition){
		//TODO
	}
	
	public void deleteOrder(pojo.Order order) {
		//TODO using the id
		return;
	}
	public void deleteOrderListInBuys(	
		conditions.Condition<conditions.OrderAttribute> order_condition,	
		conditions.Condition<conditions.CustomerAttribute> client_condition){
			//TODO
		}
	
	public void deleteOrderListInBuysByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteOrderListInBuys(order_condition, null);
	}
	public void deleteOrderListInBuysByClientCondition(
		conditions.Condition<conditions.CustomerAttribute> client_condition
	){
		deleteOrderListInBuys(null, client_condition);
	}
	
	public void deleteOrderListInBuysByClient(
		pojo.Customer client 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteOrderPListInComposed_of(	
		conditions.Condition<conditions.OrderAttribute> orderP_condition,	
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition){
			//TODO
		}
	
	public void deleteOrderPListInComposed_ofByOrderPCondition(
		conditions.Condition<conditions.OrderAttribute> orderP_condition
	){
		deleteOrderPListInComposed_of(orderP_condition, null);
	}
	public void deleteOrderPListInComposed_ofByOrderedProductsCondition(
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition
	){
		deleteOrderPListInComposed_of(null, orderedProducts_condition);
	}
	
	public void deleteOrderPListInComposed_ofByOrderedProducts(
		pojo.Product orderedProducts 
	){
		//TODO get id in condition
		return;	
	}
	
	
}
