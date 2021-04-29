package dao.impl;

import java.util.Arrays;
import java.util.List;
import pojo.Order;
import conditions.*;
import dao.services.OrderService;
import util.Util;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
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

public class OrderServiceImpl extends OrderService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OrderServiceImpl.class);
	
	
	
	public static String getBSONMatchQueryInClientCollectionFromMongo(Condition<OrderAttribute> condition, MutableBoolean refilterFlag) {	
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
						res = "orderId': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "orders." + res;
					res = "'" + res;
					}
					if(attr == OrderAttribute.quantity ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "qty': {" + mongoOp + ": " + preparedValue + "}";
	
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
				String bsonLeft = getBSONMatchQueryInClientCollectionFromMongo(((AndCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInClientCollectionFromMongo(((AndCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $and: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";
			}
	
			if(condition instanceof OrCondition) {
				String bsonLeft = getBSONMatchQueryInClientCollectionFromMongo(((OrCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInClientCollectionFromMongo(((OrCondition)condition).getRightCondition(), refilterFlag);			
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
	
	public Dataset<Order> getOrderListInClientCollectionFromMongo(conditions.Condition<conditions.OrderAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = OrderServiceImpl.getBSONMatchQueryInClientCollectionFromMongo(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mongo", "ClientCollection", bsonQuery);
	
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
							// 	attribute Order.id for field orderId			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("orderId")) {
								if(nestedRow.getAs("orderId")==null)
									order2.setId(null);
								else{
									order2.setId((Integer) nestedRow.getAs("orderId"));
									toAdd2 = true;					
									}
							}
							// 	attribute Order.quantity for field qty			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("qty")) {
								if(nestedRow.getAs("qty")==null)
									order2.setQuantity(null);
								else{
									order2.setQuantity((Integer) nestedRow.getAs("qty"));
									toAdd2 = true;					
									}
							}
							if(toAdd2) {
								if(condition ==null || refilterFlag.booleanValue() || condition.evaluate(order2))
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
	
	
	// TODO get based on id(s). Ex:public Client getClientById(Long id)
	
	public Dataset<Order> getOrderListById(Integer id) {
		return getOrderList(conditions.Condition.simple(conditions.OrderAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Order> getOrderListByQuantity(Integer quantity) {
		return getOrderList(conditions.Condition.simple(conditions.OrderAttribute.quantity, conditions.Operator.EQUALS, quantity));
	}
	
	
	
	
	public Dataset<Order> getOrderListInPlaces(conditions.Condition<conditions.CustomerAttribute> buyer_condition,conditions.Condition<conditions.OrderAttribute> order_condition)		{
		MutableBoolean order_refilter = new MutableBoolean(false);
		List<Dataset<Order>> datasetsPOJO = new ArrayList<Dataset<Order>>();
		Dataset<Customer> all = new CustomerServiceImpl().getCustomerList(buyer_condition);
		boolean all_already_persisted = false;
		MutableBoolean buyer_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		
		// Role 'buyer' mapped to EmbeddedObject 'orders' 'Order' containing 'Customer' 
		buyer_refilter = new MutableBoolean(false);
		Dataset<Order> res_Order;
		Dataset<Places> res_places_order;
		res_places_order = placesService.getPlacesListInmongoSchemaClientCollectionorders(buyer_condition, order_condition, buyer_refilter, order_refilter);
		if(buyer_refilter.booleanValue()) {
			joinCondition = null;
			joinCondition = res_places_order.col("buyer.id").equalTo(all.col("id"));
			if(joinCondition == null)
				res_Order = res_places_order.join(all).select("order.*").as(Encoders.bean(Order.class));
			else
				res_Order = res_places_order.join(all, joinCondition).select("order.*").as(Encoders.bean(Order.class));
		
		} else
			res_Order = res_places_order.map((MapFunction<Places,Order>) r -> r.getOrder(), Encoders.bean(Order.class));
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
	public Dataset<Order> getOrderListInPlacesByBuyerCondition(conditions.Condition<conditions.CustomerAttribute> buyer_condition){
		return getOrderListInPlaces(buyer_condition, null);
	}
	
	public Dataset<Order> getOrderListInPlacesByBuyer(pojo.Customer buyer){
		if(buyer == null)
			return null;
	
		Condition c;
		c=Condition.simple(CustomerAttribute.id,Operator.EQUALS, buyer.getId());
		Dataset<Order> res = getOrderListInPlacesByBuyerCondition(c);
		return res;
	}
	
	public Dataset<Order> getOrderListInPlacesByOrderCondition(conditions.Condition<conditions.OrderAttribute> order_condition){
		return getOrderListInPlaces(null, order_condition);
	}
	public Dataset<Order> getOrderListInOf(conditions.Condition<conditions.ProductAttribute> bought_item_condition,conditions.Condition<conditions.OrderAttribute> order_condition)		{
		MutableBoolean order_refilter = new MutableBoolean(false);
		List<Dataset<Order>> datasetsPOJO = new ArrayList<Dataset<Order>>();
		Dataset<Product> all = new ProductServiceImpl().getProductList(bought_item_condition);
		boolean all_already_persisted = false;
		MutableBoolean bought_item_refilter;
		org.apache.spark.sql.Column joinCondition = null;
	
		// For role 'order' in reference 'buys' 
		Dataset<OrderTDO> orderTDObuysorder = ofService.getOrderTDOListOrderInBuysInClientCollectionFromMongoSchema(order_condition, order_refilter);
		bought_item_refilter = new MutableBoolean(false);
		Dataset<ProductTDO> productTDObuysbought_item = ofService.getProductTDOListBought_itemInBuysInClientCollectionFromMongoSchema(bought_item_condition, bought_item_refilter);
		if(bought_item_refilter.booleanValue()) {
			joinCondition = null;
			joinCondition = productTDObuysbought_item.col("id").equalTo(all.col("id"));
			if(joinCondition == null)
				productTDObuysbought_item = productTDObuysbought_item.as("A").join(all).select("A.*").as(Encoders.bean(ProductTDO.class));
			else
				productTDObuysbought_item = productTDObuysbought_item.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(ProductTDO.class));
		}
	
		Dataset<Row> res_buys = orderTDObuysorder.join(productTDObuysbought_item
				.withColumnRenamed("id", "Product_id")
				.withColumnRenamed("label", "Product_label")
				.withColumnRenamed("price", "Product_price")
				.withColumnRenamed("logEvents", "Product_logEvents"),
				orderTDObuysorder.col("mongoSchema_ClientCollection_buys_productId").equalTo(productTDObuysbought_item.col("mongoSchema_ClientCollection_buys_ID")));
		Dataset<Order> res_Order_buys = res_buys.select( "id", "quantity", "logEvents").as(Encoders.bean(Order.class));
		
		res_Order_buys = res_Order_buys.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Order_buys);
		
		
		
		
		//Join datasets or return 
		Dataset<Order> res = fullOuterJoinsOrder(datasetsPOJO);
		if(res == null)
			return null;
	
		if(order_refilter.booleanValue())
			res = res.filter((FilterFunction<Order>) r -> order_condition == null || order_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Order> getOrderListInOfByBought_itemCondition(conditions.Condition<conditions.ProductAttribute> bought_item_condition){
		return getOrderListInOf(bought_item_condition, null);
	}
	
	public Dataset<Order> getOrderListInOfByBought_item(pojo.Product bought_item){
		if(bought_item == null)
			return null;
	
		Condition c;
		c=Condition.simple(ProductAttribute.id,Operator.EQUALS, bought_item.getId());
		Dataset<Order> res = getOrderListInOfByBought_itemCondition(c);
		return res;
	}
	
	public Dataset<Order> getOrderListInOfByOrderCondition(conditions.Condition<conditions.OrderAttribute> order_condition){
		return getOrderListInOf(null, order_condition);
	}
	public Dataset<Order> getOrderListInFrom(conditions.Condition<conditions.StoreAttribute> store_condition,conditions.Condition<conditions.OrderAttribute> order_condition)		{
		MutableBoolean order_refilter = new MutableBoolean(false);
		List<Dataset<Order>> datasetsPOJO = new ArrayList<Dataset<Order>>();
		Dataset<Store> all = new StoreServiceImpl().getStoreList(store_condition);
		boolean all_already_persisted = false;
		MutableBoolean store_refilter;
		org.apache.spark.sql.Column joinCondition = null;
	
		// For role 'order' in reference 'buys_in' 
		Dataset<OrderTDO> orderTDObuys_inorder = fromService.getOrderTDOListOrderInBuys_inInClientCollectionFromMongoSchema(order_condition, order_refilter);
		store_refilter = new MutableBoolean(false);
		Dataset<StoreTDO> storeTDObuys_instore = fromService.getStoreTDOListStoreInBuys_inInClientCollectionFromMongoSchema(store_condition, store_refilter);
		if(store_refilter.booleanValue()) {
			joinCondition = null;
			joinCondition = storeTDObuys_instore.col("id").equalTo(all.col("id"));
			if(joinCondition == null)
				storeTDObuys_instore = storeTDObuys_instore.as("A").join(all).select("A.*").as(Encoders.bean(StoreTDO.class));
			else
				storeTDObuys_instore = storeTDObuys_instore.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(StoreTDO.class));
		}
	
		Dataset<Row> res_buys_in = orderTDObuys_inorder.join(storeTDObuys_instore
				.withColumnRenamed("id", "Store_id")
				.withColumnRenamed("vAT", "Store_vAT")
				.withColumnRenamed("address", "Store_address")
				.withColumnRenamed("logEvents", "Store_logEvents"),
				orderTDObuys_inorder.col("mongoSchema_ClientCollection_buys_in_storeId").equalTo(storeTDObuys_instore.col("mongoSchema_ClientCollection_buys_in_ID")));
		Dataset<Order> res_Order_buys_in = res_buys_in.select( "id", "quantity", "logEvents").as(Encoders.bean(Order.class));
		
		res_Order_buys_in = res_Order_buys_in.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Order_buys_in);
		
		
		
		
		//Join datasets or return 
		Dataset<Order> res = fullOuterJoinsOrder(datasetsPOJO);
		if(res == null)
			return null;
	
		if(order_refilter.booleanValue())
			res = res.filter((FilterFunction<Order>) r -> order_condition == null || order_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Order> getOrderListInFromByStoreCondition(conditions.Condition<conditions.StoreAttribute> store_condition){
		return getOrderListInFrom(store_condition, null);
	}
	
	public Dataset<Order> getOrderListInFromByStore(pojo.Store store){
		if(store == null)
			return null;
	
		Condition c;
		c=Condition.simple(StoreAttribute.id,Operator.EQUALS, store.getId());
		Dataset<Order> res = getOrderListInFromByStoreCondition(c);
		return res;
	}
	
	public Dataset<Order> getOrderListInFromByOrderCondition(conditions.Condition<conditions.OrderAttribute> order_condition){
		return getOrderListInFrom(null, order_condition);
	}
	
	public void insertOrderAndLinkedItems(Order order){
		//TODO
	}
	public void insertOrder(
		Order order,
		pojo.Customer persistentPlacesBuyer,
		pojo.Product persistentOfBought_item,
		pojo.Store persistentFromStore){
			//TODO
		}
	
	public void insertOrderInClientCollectionFromMongo(Order order){
		//Read mapping rules and find attributes of the POJO that are mapped to the corresponding AbstractPhysicalStructure
		// Insert in MongoDB
	}
	
	public void updateOrderList(conditions.Condition<conditions.OrderAttribute> condition, conditions.SetClause<conditions.OrderAttribute> set){
		//TODO
	}
	
	public void updateOrder(pojo.Order order) {
		//TODO using the id
		return;
	}
	public void updateOrderListInPlaces(
		conditions.Condition<conditions.CustomerAttribute> buyer_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition,
		
		conditions.SetClause<conditions.OrderAttribute> set
	){
		//TODO
	}
	
	public void updateOrderListInPlacesByBuyerCondition(
		conditions.Condition<conditions.CustomerAttribute> buyer_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInPlaces(buyer_condition, null, set);
	}
	
	public void updateOrderListInPlacesByBuyer(
		pojo.Customer buyer,
		conditions.SetClause<conditions.OrderAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateOrderListInPlacesByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInPlaces(null, order_condition, set);
	}
	public void updateOrderListInOf(
		conditions.Condition<conditions.ProductAttribute> bought_item_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition,
		
		conditions.SetClause<conditions.OrderAttribute> set
	){
		//TODO
	}
	
	public void updateOrderListInOfByBought_itemCondition(
		conditions.Condition<conditions.ProductAttribute> bought_item_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInOf(bought_item_condition, null, set);
	}
	
	public void updateOrderListInOfByBought_item(
		pojo.Product bought_item,
		conditions.SetClause<conditions.OrderAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateOrderListInOfByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInOf(null, order_condition, set);
	}
	public void updateOrderListInFrom(
		conditions.Condition<conditions.StoreAttribute> store_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition,
		
		conditions.SetClause<conditions.OrderAttribute> set
	){
		//TODO
	}
	
	public void updateOrderListInFromByStoreCondition(
		conditions.Condition<conditions.StoreAttribute> store_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInFrom(store_condition, null, set);
	}
	
	public void updateOrderListInFromByStore(
		pojo.Store store,
		conditions.SetClause<conditions.OrderAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateOrderListInFromByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInFrom(null, order_condition, set);
	}
	
	
	public void deleteOrderList(conditions.Condition<conditions.OrderAttribute> condition){
		//TODO
	}
	
	public void deleteOrder(pojo.Order order) {
		//TODO using the id
		return;
	}
	public void deleteOrderListInPlaces(	
		conditions.Condition<conditions.CustomerAttribute> buyer_condition,	
		conditions.Condition<conditions.OrderAttribute> order_condition){
			//TODO
		}
	
	public void deleteOrderListInPlacesByBuyerCondition(
		conditions.Condition<conditions.CustomerAttribute> buyer_condition
	){
		deleteOrderListInPlaces(buyer_condition, null);
	}
	
	public void deleteOrderListInPlacesByBuyer(
		pojo.Customer buyer 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteOrderListInPlacesByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteOrderListInPlaces(null, order_condition);
	}
	public void deleteOrderListInOf(	
		conditions.Condition<conditions.ProductAttribute> bought_item_condition,	
		conditions.Condition<conditions.OrderAttribute> order_condition){
			//TODO
		}
	
	public void deleteOrderListInOfByBought_itemCondition(
		conditions.Condition<conditions.ProductAttribute> bought_item_condition
	){
		deleteOrderListInOf(bought_item_condition, null);
	}
	
	public void deleteOrderListInOfByBought_item(
		pojo.Product bought_item 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteOrderListInOfByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteOrderListInOf(null, order_condition);
	}
	public void deleteOrderListInFrom(	
		conditions.Condition<conditions.StoreAttribute> store_condition,	
		conditions.Condition<conditions.OrderAttribute> order_condition){
			//TODO
		}
	
	public void deleteOrderListInFromByStoreCondition(
		conditions.Condition<conditions.StoreAttribute> store_condition
	){
		deleteOrderListInFrom(store_condition, null);
	}
	
	public void deleteOrderListInFromByStore(
		pojo.Store store 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteOrderListInFromByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteOrderListInFrom(null, order_condition);
	}
	
}
