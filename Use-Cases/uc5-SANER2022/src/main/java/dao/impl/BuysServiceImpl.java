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
import conditions.BuysAttribute;
import conditions.Operator;
import pojo.Buys;
import tdo.OrderTDO;
import tdo.BuysTDO;
import pojo.Order;
import conditions.OrderAttribute;
import tdo.CustomerTDO;
import tdo.BuysTDO;
import pojo.Customer;
import conditions.CustomerAttribute;
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

public class BuysServiceImpl extends dao.services.BuysService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BuysServiceImpl.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	
	
	// Left side 'PersonId' of reference [customer ]
	public Dataset<OrderTDO> getOrderTDOListOrderInCustomerInOrdersColFromDocSchema(Condition<OrderAttribute> condition, MutableBoolean refilterFlag){	
		String bsonQuery = OrderServiceImpl.getBSONMatchQueryInOrdersColFromMongobench(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mongobench", "ordersCol", bsonQuery);
	
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
					// 	attribute Order.id for field OrderId			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderId")) {
						if(nestedRow.getAs("OrderId") == null){
							order1.setId(null);
						}else{
							order1.setId(Util.getStringValue(nestedRow.getAs("OrderId")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.orderdate for field OrderDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderDate")) {
						if(nestedRow.getAs("OrderDate") == null){
							order1.setOrderdate(null);
						}else{
							order1.setOrderdate(Util.getLocalDateValue(nestedRow.getAs("OrderDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.totalprice for field TotalPrice			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TotalPrice")) {
						if(nestedRow.getAs("TotalPrice") == null){
							order1.setTotalprice(null);
						}else{
							order1.setTotalprice(Util.getDoubleValue(nestedRow.getAs("TotalPrice")));
							toAdd1 = true;					
							}
					}
					
						// field  PersonId for reference customer . Reference field : PersonId
					nestedRow =  r1;
					if(nestedRow != null) {
						order1.setDocSchema_ordersCol_customer_PersonId(nestedRow.getAs("PersonId") == null ? null : nestedRow.getAs("PersonId").toString());
						toAdd1 = true;					
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
	
	// Right side 'id' of reference [customer ]
	public Dataset<CustomerTDO> getCustomerTDOListClientInCustomerInOrdersColFromDocSchema(Condition<CustomerAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = CustomerServiceImpl.getSQLWhereClauseInCustomerTableFromMysqlbench(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", "'" + Util.escapeQuote(preparedValue) + "'");
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("mysqlbench", "customerTable", where);
		
	
		Dataset<CustomerTDO> res = d.map((MapFunction<Row, CustomerTDO>) r -> {
					CustomerTDO customer_res = new CustomerTDO();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Customer.Id]
					String id = Util.getStringValue(r.getAs("id"));
					customer_res.setId(id);
					
					// attribute [Customer.Firstname]
					String firstname = Util.getStringValue(r.getAs("firstName"));
					customer_res.setFirstname(firstname);
					
					// attribute [Customer.Lastname]
					String lastname = Util.getStringValue(r.getAs("lastName"));
					customer_res.setLastname(lastname);
					
					// attribute [Customer.Gender]
					String gender = Util.getStringValue(r.getAs("gender"));
					customer_res.setGender(gender);
					
					// attribute [Customer.Birthday]
					LocalDate birthday = Util.getLocalDateValue(r.getAs("birthday"));
					customer_res.setBirthday(birthday);
					
					// attribute [Customer.CreationDate]
					LocalDate creationDate = Util.getLocalDateValue(r.getAs("creationDate"));
					customer_res.setCreationDate(creationDate);
					
					// attribute [Customer.Locationip]
					String locationip = Util.getStringValue(r.getAs("locationIP"));
					customer_res.setLocationip(locationip);
					
					// attribute [Customer.Browser]
					String browser = Util.getStringValue(r.getAs("browserUsed"));
					customer_res.setBrowser(browser);
	
					// Get reference column [id ] for reference [customer]
					String docSchema_ordersCol_customer_id = r.getAs("id") == null ? null : r.getAs("id").toString();
					customer_res.setDocSchema_ordersCol_customer_id(docSchema_ordersCol_customer_id);
	
	
					return customer_res;
				}, Encoders.bean(CustomerTDO.class));
	
	
		return res;}
	
	
	
	
	
	
	public java.util.List<pojo.Buys> getBuysList(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.CustomerAttribute> client_condition){
			//TODO
			return null;
		}
	
	public java.util.List<pojo.Buys> getBuysListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		return getBuysList(order_condition, null);
	}
	
	public pojo.Buys getBuysByOrder(pojo.Order order) {
		// TODO using id for selecting
		return null;
	}
	public java.util.List<pojo.Buys> getBuysListByClientCondition(
		conditions.Condition<conditions.CustomerAttribute> client_condition
	){
		return getBuysList(null, client_condition);
	}
	
	public java.util.List<pojo.Buys> getBuysListByClient(pojo.Customer client) {
		// TODO using id for selecting
		return null;
	}
	
	
	
	public void deleteBuysList(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.CustomerAttribute> client_condition){
			//TODO
		}
	
	public void deleteBuysListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteBuysList(order_condition, null);
	}
	
	public void deleteBuysByOrder(pojo.Order order) {
		// TODO using id for selecting
		return;
	}
	public void deleteBuysListByClientCondition(
		conditions.Condition<conditions.CustomerAttribute> client_condition
	){
		deleteBuysList(null, client_condition);
	}
	
	public void deleteBuysListByClient(pojo.Customer client) {
		// TODO using id for selecting
		return;
	}
		
}
