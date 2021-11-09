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
	
	
	
	
	public static String getBSONMatchQueryInOrdersColFromMongobench(Condition<OrderAttribute> condition, MutableBoolean refilterFlag) {	
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
						res = "OrderId': {" + mongoOp + ": " + preparedValue + "}";
	
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
						res = "OrderDate': {" + mongoOp + ": " + preparedValue + "}";
	
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
						res = "TotalPrice': {" + mongoOp + ": " + preparedValue + "}";
	
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
	
	public Dataset<Order> getOrderListInOrdersColFromMongobench(conditions.Condition<conditions.OrderAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = OrderServiceImpl.getBSONMatchQueryInOrdersColFromMongobench(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mongobench", "ordersCol", bsonQuery);
	
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
					// 	attribute Order.id for field OrderId			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderId")) {
						if(nestedRow.getAs("OrderId")==null)
							order1.setId(null);
						else{
							order1.setId(Util.getStringValue(nestedRow.getAs("OrderId")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.orderdate for field OrderDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderDate")) {
						if(nestedRow.getAs("OrderDate")==null)
							order1.setOrderdate(null);
						else{
							order1.setOrderdate(Util.getLocalDateValue(nestedRow.getAs("OrderDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.totalprice for field TotalPrice			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TotalPrice")) {
						if(nestedRow.getAs("TotalPrice")==null)
							order1.setTotalprice(null);
						else{
							order1.setTotalprice(Util.getDoubleValue(nestedRow.getAs("TotalPrice")));
							toAdd1 = true;					
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
	
	
	// TODO get based on id(s). Ex:public Client getClientById(Long id)
	public Order getOrderById(String id){
		Condition cond;
		cond = Condition.simple(OrderAttribute.id, conditions.Operator.EQUALS, id);
		Dataset<Order> res = getOrderList(cond);
		if(res!=null)
			return res.first();
		return null;
	}
	
	public Dataset<Order> getOrderListById(String id) {
		return getOrderList(conditions.Condition.simple(conditions.OrderAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Order> getOrderListByOrderdate(LocalDate orderdate) {
		return getOrderList(conditions.Condition.simple(conditions.OrderAttribute.orderdate, conditions.Operator.EQUALS, orderdate));
	}
	
	public Dataset<Order> getOrderListByTotalprice(Double totalprice) {
		return getOrderList(conditions.Condition.simple(conditions.OrderAttribute.totalprice, conditions.Operator.EQUALS, totalprice));
	}
	
	
	
	
	public Dataset<Order> getOrderListInBuys(conditions.Condition<conditions.OrderAttribute> order_condition,conditions.Condition<conditions.CustomerAttribute> client_condition)		{
		MutableBoolean order_refilter = new MutableBoolean(false);
		List<Dataset<Order>> datasetsPOJO = new ArrayList<Dataset<Order>>();
		Dataset<Customer> all = null;
		boolean all_already_persisted = false;
		MutableBoolean client_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		// For role 'order' in reference 'customer'. A->B Scenario
		client_refilter = new MutableBoolean(false);
		Dataset<OrderTDO> orderTDOcustomerorder = buysService.getOrderTDOListOrderInCustomerInOrdersColFromDocSchema(order_condition, order_refilter);
		Dataset<CustomerTDO> customerTDOcustomerclient = buysService.getCustomerTDOListClientInCustomerInOrdersColFromDocSchema(client_condition, client_refilter);
		if(client_refilter.booleanValue()) {
			if(all == null)
				all = new CustomerServiceImpl().getCustomerList(client_condition);
			joinCondition = null;
			joinCondition = customerTDOcustomerclient.col("id").equalTo(all.col("id"));
			if(joinCondition == null)
				customerTDOcustomerclient = customerTDOcustomerclient.as("A").join(all).select("A.*").as(Encoders.bean(CustomerTDO.class));
			else
				customerTDOcustomerclient = customerTDOcustomerclient.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(CustomerTDO.class));
		}
	
		
		Dataset<Row> res_customer = orderTDOcustomerorder.join(customerTDOcustomerclient
				.withColumnRenamed("id", "Customer_id")
				.withColumnRenamed("firstname", "Customer_firstname")
				.withColumnRenamed("lastname", "Customer_lastname")
				.withColumnRenamed("gender", "Customer_gender")
				.withColumnRenamed("birthday", "Customer_birthday")
				.withColumnRenamed("creationDate", "Customer_creationDate")
				.withColumnRenamed("locationip", "Customer_locationip")
				.withColumnRenamed("browser", "Customer_browser")
				.withColumnRenamed("logEvents", "Customer_logEvents"),
				orderTDOcustomerorder.col("docSchema_ordersCol_customer_PersonId").equalTo(customerTDOcustomerclient.col("docSchema_ordersCol_customer_id")));
		Dataset<Order> res_Order_customer = res_customer.select( "id", "orderdate", "totalprice", "logEvents").as(Encoders.bean(Order.class));
		
		res_Order_customer = res_Order_customer.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Order_customer);
		
		
		Dataset<Buys> res_buys_order;
		Dataset<Order> res_Order;
		
		
		//Join datasets or return 
		Dataset<Order> res = fullOuterJoinsOrder(datasetsPOJO);
		if(res == null)
			return null;
	
		if(order_refilter.booleanValue())
			res = res.filter((FilterFunction<Order>) r -> order_condition == null || order_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Order> getOrderListInBuysByOrderCondition(conditions.Condition<conditions.OrderAttribute> order_condition){
		return getOrderListInBuys(order_condition, null);
	}
	public Dataset<Order> getOrderListInBuysByClientCondition(conditions.Condition<conditions.CustomerAttribute> client_condition){
		return getOrderListInBuys(null, client_condition);
	}
	
	public Dataset<Order> getOrderListInBuysByClient(pojo.Customer client){
		if(client == null)
			return null;
	
		Condition c;
		c=Condition.simple(CustomerAttribute.id,Operator.EQUALS, client.getId());
		Dataset<Order> res = getOrderListInBuysByClientCondition(c);
		return res;
	}
	
	public Dataset<Order> getOrderPListInComposed_of(conditions.Condition<conditions.OrderAttribute> orderP_condition,conditions.Condition<conditions.ProductAttribute> orderedProducts_condition)		{
		MutableBoolean orderP_refilter = new MutableBoolean(false);
		List<Dataset<Order>> datasetsPOJO = new ArrayList<Dataset<Order>>();
		Dataset<Product> all = null;
		boolean all_already_persisted = false;
		MutableBoolean orderedProducts_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		
		Dataset<Composed_of> res_composed_of_orderP;
		Dataset<Order> res_Order;
		// Role 'orderP' mapped to EmbeddedObject 'Orderline' - 'Product' containing 'Order'
		orderedProducts_refilter = new MutableBoolean(false);
		res_composed_of_orderP = composed_ofService.getComposed_ofListIndocSchemaordersColOrderline(orderP_condition, orderedProducts_condition, orderP_refilter, orderedProducts_refilter);
		if(orderedProducts_refilter.booleanValue()) {
			if(all == null)
				all = new ProductServiceImpl().getProductList(orderedProducts_condition);
			joinCondition = null;
			joinCondition = res_composed_of_orderP.col("orderedProducts.id").equalTo(all.col("id"));
			if(joinCondition == null)
				res_Order = res_composed_of_orderP.join(all).select("orderP.*").as(Encoders.bean(Order.class));
			else
				res_Order = res_composed_of_orderP.join(all, joinCondition).select("orderP.*").as(Encoders.bean(Order.class));
		
		} else
			res_Order = res_composed_of_orderP.map((MapFunction<Composed_of,Order>) r -> r.getOrderP(), Encoders.bean(Order.class));
		res_Order = res_Order.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Order);
		
		
		//Join datasets or return 
		Dataset<Order> res = fullOuterJoinsOrder(datasetsPOJO);
		if(res == null)
			return null;
	
		if(orderP_refilter.booleanValue())
			res = res.filter((FilterFunction<Order>) r -> orderP_condition == null || orderP_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Order> getOrderPListInComposed_ofByOrderPCondition(conditions.Condition<conditions.OrderAttribute> orderP_condition){
		return getOrderPListInComposed_of(orderP_condition, null);
	}
	public Dataset<Order> getOrderPListInComposed_ofByOrderedProductsCondition(conditions.Condition<conditions.ProductAttribute> orderedProducts_condition){
		return getOrderPListInComposed_of(null, orderedProducts_condition);
	}
	
	public Dataset<Order> getOrderPListInComposed_ofByOrderedProducts(pojo.Product orderedProducts){
		if(orderedProducts == null)
			return null;
	
		Condition c;
		c=Condition.simple(ProductAttribute.id,Operator.EQUALS, orderedProducts.getId());
		Dataset<Order> res = getOrderPListInComposed_ofByOrderedProductsCondition(c);
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
		 	inserted = insertOrderInOrdersColFromMongobench(order,clientBuys,orderedProductsComposed_of)|| inserted ;
		 	// Insert in ascending structures 
		 	// Insert in ref structures 
		 	// Insert in ref structures mapped to opposite role of mandatory role  
		 	return inserted;
		 }
	
	public boolean insertOrderInOrdersColFromMongobench(Order order,
		Customer	clientBuys,
		 List<Product> orderedProductsComposed_of)	{
			 // Implement Insert in descending complex struct
			Bson filter = new Document();
			Bson updateOp;
			Document docordersCol_1 = new Document();
			docordersCol_1.append("OrderId",order.getId());
			// Ref 'customer' mapped to mandatory role 'order'
			docordersCol_1.append("PersonId",clientBuys.getId());
			docordersCol_1.append("OrderDate",order.getOrderdate());
			docordersCol_1.append("TotalPrice",order.getTotalprice());
			// field 'Orderline' is mapped to mandatory role 'orderP' with opposite role of type 'Product'
				List<Document> arrayOrderline_1 = new ArrayList();
					for(Product product : orderedProductsComposed_of){
						Document docOrderline_2 = new Document();
						docOrderline_2.append("asin",product.getId());
						docOrderline_2.append("title",product.getTitle());
						docOrderline_2.append("price",product.getPrice());
						
						arrayOrderline_1.add(docOrderline_2);
					}
				docordersCol_1.append("Orderline", arrayOrderline_1);
			
			filter = eq("OrderId",order.getId());
			updateOp = setOnInsert(docordersCol_1);
			DBConnectionMgr.upsertMany(filter, updateOp, "ordersCol", "mongobench");
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
