package dao.impl;

import java.util.Arrays;
import org.apache.spark.sql.Dataset;
import conditions.Condition;
import conditions.AndCondition;
import conditions.OrCondition;
import conditions.SimpleCondition;
import conditions.FromAttribute;
import conditions.Operator;
import pojo.From;
import tdo.StoreTDO;
import tdo.FromTDO;
import pojo.Store;
import conditions.StoreAttribute;
import tdo.OrderTDO;
import tdo.FromTDO;
import pojo.Order;
import conditions.OrderAttribute;
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


public class FromServiceImpl extends dao.services.FromService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FromServiceImpl.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	
	
	// Left side 'storeId' of reference [buys_in ]
	public Dataset<OrderTDO> getOrderTDOListOrderInBuys_inInClientCollectionFromMongoSchema(Condition<OrderAttribute> condition, MutableBoolean refilterFlag){	
		String bsonQuery = OrderServiceImpl.getBSONMatchQueryInClientCollectionFromMongo(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mongo", "ClientCollection", bsonQuery);
	
		Dataset<OrderTDO> res = dataset.flatMap((FlatMapFunction<Row, OrderTDO>) r -> {
				List<OrderTDO> list_res = new ArrayList<OrderTDO>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				OrderTDO order1 = new OrderTDO();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					
					
					
					array1 = r1.getAs("orders");
					if(array1!= null) {
						for (int i2 = 0; i2 < array1.size(); i2++){
							Row r2 = (Row) array1.apply(i2);
							OrderTDO order2 = (OrderTDO) order1.clone();
							boolean toAdd2  = false;
							WrappedArray array2  = null;
							// 	attribute Order.id for field orderId			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("orderId")) {
								if(nestedRow.getAs("orderId") == null){
									order2.setId(null);
								}else{
									order2.setId((Integer) nestedRow.getAs("orderId"));
									toAdd2 = true;					
									}
							}
							// 	attribute Order.quantity for field qty			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("qty")) {
								if(nestedRow.getAs("qty") == null){
									order2.setQuantity(null);
								}else{
									order2.setQuantity((Integer) nestedRow.getAs("qty"));
									toAdd2 = true;					
									}
							}
							
								// field  storeId for reference buys_in
							nestedRow =  r2;
							if(nestedRow != null) {
								order2.setMongoSchema_ClientCollection_buys_in_storeId(nestedRow.getAs("storeId") == null ? null : nestedRow.getAs("storeId").toString());
								toAdd2 = true;					
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
	
		}, Encoders.bean(OrderTDO.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
	}
	
	// Right side 'ID' of reference [buys_in ]
	public Dataset<StoreTDO> getStoreTDOListStoreInBuys_inInClientCollectionFromMongoSchema(Condition<StoreAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = StoreServiceImpl.getSQLWhereClauseInSTOREFromINVENTORY(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", "'" + Util.escapeQuote(preparedValue) + "'");
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("INVENTORY", "STORE");
		if(where != null) {
			d = d.where(where);
		}
	
		Dataset<StoreTDO> res = d.map((MapFunction<Row, StoreTDO>) r -> {
					StoreTDO store_res = new StoreTDO();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Store.Id]
					Integer id = r.getAs("ID");
					store_res.setId(id);
					
					// attribute [Store.VAT]
					String vAT = r.getAs("VAT");
					store_res.setVAT(vAT);
					
					// attribute [Store.Address]
					String address = r.getAs("ADDR");
					store_res.setAddress(address);
	
					// Get reference column [ID ] for reference [buys_in]
					String mongoSchema_ClientCollection_buys_in_ID = r.getAs("ID") == null ? null : r.getAs("ID").toString();
					store_res.setMongoSchema_ClientCollection_buys_in_ID(mongoSchema_ClientCollection_buys_in_ID);
	
	
					return store_res;
				}, Encoders.bean(StoreTDO.class));
	
	
		return res;}
	
	
	
	
	
	
	public java.util.List<pojo.From> getFromList(
		conditions.Condition<conditions.StoreAttribute> store_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition){
			//TODO
			return null;
		}
	
	public java.util.List<pojo.From> getFromListByStoreCondition(
		conditions.Condition<conditions.StoreAttribute> store_condition
	){
		return getFromList(store_condition, null);
	}
	
	public java.util.List<pojo.From> getFromListByStore(pojo.Store store) {
		// TODO using id for selecting
		return null;
	}
	public java.util.List<pojo.From> getFromListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		return getFromList(null, order_condition);
	}
	
	public pojo.From getFromByOrder(pojo.Order order) {
		// TODO using id for selecting
		return null;
	}
	
	public void insertFromAndLinkedItems(pojo.From from){
		//TODO
	}
	
	
	
	public void deleteFromList(
		conditions.Condition<conditions.StoreAttribute> store_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition){
			//TODO
		}
	
	public void deleteFromListByStoreCondition(
		conditions.Condition<conditions.StoreAttribute> store_condition
	){
		deleteFromList(store_condition, null);
	}
	
	public void deleteFromListByStore(pojo.Store store) {
		// TODO using id for selecting
		return;
	}
	public void deleteFromListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteFromList(null, order_condition);
	}
	
	public void deleteFromByOrder(pojo.Order order) {
		// TODO using id for selecting
		return;
	}
		
}
