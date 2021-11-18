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
import dao.services.OrderService;
import tdo.CustomerTDO;
import tdo.BuysTDO;
import pojo.Customer;
import conditions.CustomerAttribute;
import dao.services.CustomerService;
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
	
	
	// Left side 'customerId' of reference [clientRef ]
	public Dataset<OrderTDO> getOrderTDOListOrderInClientRefInOrderTableFromRelSchemaB(Condition<OrderAttribute> condition, MutableBoolean refilterFlag){	
	
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
	
					// Get reference column [customerId ] for reference [clientRef]
					String relSchemaB_orderTable_clientRef_customerId = r.getAs("customerId") == null ? null : r.getAs("customerId").toString();
					order_res.setRelSchemaB_orderTable_clientRef_customerId(relSchemaB_orderTable_clientRef_customerId);
	
	
					return order_res;
				}, Encoders.bean(OrderTDO.class));
	
	
		return res;
	}
	
	// Right side 'id' of reference [clientRef ]
	public Dataset<CustomerTDO> getCustomerTDOListClientInClientRefInOrderTableFromRelSchemaB(Condition<CustomerAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = CustomerServiceImpl.getBSONMatchQueryInUserColFromMongoModelB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mongoModelB", "userCol", bsonQuery);
	
		Dataset<CustomerTDO> res = dataset.flatMap((FlatMapFunction<Row, CustomerTDO>) r -> {
				List<CustomerTDO> list_res = new ArrayList<CustomerTDO>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				CustomerTDO customer1 = new CustomerTDO();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Customer.id for field id			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("id")) {
						if(nestedRow.getAs("id") == null){
							customer1.setId(null);
						}else{
							customer1.setId(Util.getStringValue(nestedRow.getAs("id")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.firstname for field firstName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("firstName")) {
						if(nestedRow.getAs("firstName") == null){
							customer1.setFirstname(null);
						}else{
							customer1.setFirstname(Util.getStringValue(nestedRow.getAs("firstName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.lastname for field lastName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("lastName")) {
						if(nestedRow.getAs("lastName") == null){
							customer1.setLastname(null);
						}else{
							customer1.setLastname(Util.getStringValue(nestedRow.getAs("lastName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.gender for field gender			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("gender")) {
						if(nestedRow.getAs("gender") == null){
							customer1.setGender(null);
						}else{
							customer1.setGender(Util.getStringValue(nestedRow.getAs("gender")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.birthday for field birthday			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("birthday")) {
						if(nestedRow.getAs("birthday") == null){
							customer1.setBirthday(null);
						}else{
							customer1.setBirthday(Util.getLocalDateValue(nestedRow.getAs("birthday")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.creationDate for field creationDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("creationDate")) {
						if(nestedRow.getAs("creationDate") == null){
							customer1.setCreationDate(null);
						}else{
							customer1.setCreationDate(Util.getLocalDateValue(nestedRow.getAs("creationDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.locationip for field locationIP			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("locationIP")) {
						if(nestedRow.getAs("locationIP") == null){
							customer1.setLocationip(null);
						}else{
							customer1.setLocationip(Util.getStringValue(nestedRow.getAs("locationIP")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.browser for field browserUsed			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("browserUsed")) {
						if(nestedRow.getAs("browserUsed") == null){
							customer1.setBrowser(null);
						}else{
							customer1.setBrowser(Util.getStringValue(nestedRow.getAs("browserUsed")));
							toAdd1 = true;					
							}
					}
					
						// field  id for reference clientRef . Reference field : id
					nestedRow =  r1;
					if(nestedRow != null) {
						customer1.setRelSchemaB_orderTable_clientRef_id(nestedRow.getAs("id") == null ? null : nestedRow.getAs("id").toString());
						toAdd1 = true;					
					}
					
					
					if(toAdd1) {
						list_res.add(customer1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(CustomerTDO.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
	}
	
	
	
	// method accessing the embedded object orders mapped to role client
	public Dataset<pojo.Buys> getBuysListInmongoSchemaBuserColorders(Condition<CustomerAttribute> client_condition, Condition<OrderAttribute> order_condition, MutableBoolean client_refilter, MutableBoolean order_refilter){	
			List<String> bsons = new ArrayList<String>();
			String bson = null;
			bson = OrderServiceImpl.getBSONMatchQueryInUserColFromMongoModelB(order_condition ,order_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
			bson = CustomerServiceImpl.getBSONMatchQueryInUserColFromMongoModelB(client_condition ,client_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
		
			String bsonQuery = bsons.size() == 0 ? null : "{$match: { $and: [" + String.join(",", bsons) + "] }}";
		
			Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mongoModelB", "userCol", bsonQuery);
		
			Dataset<Buys> res = dataset.flatMap((FlatMapFunction<Row, Buys>) r -> {
					List<Buys> list_res = new ArrayList<Buys>();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					Row nestedRow = null;
		
					boolean addedInList = false;
					Row r1 = r;
					Buys buys1 = new Buys();
					buys1.setOrder(new Order());
					buys1.setClient(new Customer());
					
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Customer.id for field id			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("id")) {
						if(nestedRow.getAs("id")==null)
							buys1.getClient().setId(null);
						else{
							buys1.getClient().setId(Util.getStringValue(nestedRow.getAs("id")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.firstname for field firstName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("firstName")) {
						if(nestedRow.getAs("firstName")==null)
							buys1.getClient().setFirstname(null);
						else{
							buys1.getClient().setFirstname(Util.getStringValue(nestedRow.getAs("firstName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.lastname for field lastName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("lastName")) {
						if(nestedRow.getAs("lastName")==null)
							buys1.getClient().setLastname(null);
						else{
							buys1.getClient().setLastname(Util.getStringValue(nestedRow.getAs("lastName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.gender for field gender			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("gender")) {
						if(nestedRow.getAs("gender")==null)
							buys1.getClient().setGender(null);
						else{
							buys1.getClient().setGender(Util.getStringValue(nestedRow.getAs("gender")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.birthday for field birthday			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("birthday")) {
						if(nestedRow.getAs("birthday")==null)
							buys1.getClient().setBirthday(null);
						else{
							buys1.getClient().setBirthday(Util.getLocalDateValue(nestedRow.getAs("birthday")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.creationDate for field creationDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("creationDate")) {
						if(nestedRow.getAs("creationDate")==null)
							buys1.getClient().setCreationDate(null);
						else{
							buys1.getClient().setCreationDate(Util.getLocalDateValue(nestedRow.getAs("creationDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.locationip for field locationIP			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("locationIP")) {
						if(nestedRow.getAs("locationIP")==null)
							buys1.getClient().setLocationip(null);
						else{
							buys1.getClient().setLocationip(Util.getStringValue(nestedRow.getAs("locationIP")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.browser for field browserUsed			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("browserUsed")) {
						if(nestedRow.getAs("browserUsed")==null)
							buys1.getClient().setBrowser(null);
						else{
							buys1.getClient().setBrowser(Util.getStringValue(nestedRow.getAs("browserUsed")));
							toAdd1 = true;					
							}
					}
					array1 = r1.getAs("orders");
					if(array1!= null) {
						for (int i2 = 0; i2 < array1.size(); i2++){
							Row r2 = (Row) array1.apply(i2);
							Buys buys2 = (Buys) buys1.clone();
							boolean toAdd2  = false;
							WrappedArray array2  = null;
							// 	attribute Order.id for field id			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("id")) {
								if(nestedRow.getAs("id")==null)
									buys2.getOrder().setId(null);
								else{
									buys2.getOrder().setId(Util.getStringValue(nestedRow.getAs("id")));
									toAdd2 = true;					
									}
							}
							// 	attribute Order.orderdate for field buydate			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("buydate")) {
								if(nestedRow.getAs("buydate")==null)
									buys2.getOrder().setOrderdate(null);
								else{
									buys2.getOrder().setOrderdate(Util.getLocalDateValue(nestedRow.getAs("buydate")));
									toAdd2 = true;					
									}
							}
							// 	attribute Order.totalprice for field totalamount			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("totalamount")) {
								if(nestedRow.getAs("totalamount")==null)
									buys2.getOrder().setTotalprice(null);
								else{
									buys2.getOrder().setTotalprice(Util.getDoubleValue(nestedRow.getAs("totalamount")));
									toAdd2 = true;					
									}
							}
							if(toAdd2 && ((order_condition == null || order_refilter.booleanValue() || order_condition.evaluate(buys2.getOrder()))&&(client_condition == null || client_refilter.booleanValue() || client_condition.evaluate(buys2.getClient())))) {
								list_res.add(buys2);
								addedInList = true;
							} 
							if(addedInList)
								toAdd1 = false;
						}
					}
					
					if(toAdd1 ) {
						list_res.add(buys1);
						addedInList = true;
					} 
					
					
					
					return list_res.iterator();
		
			}, Encoders.bean(Buys.class));
			// TODO drop duplicates based on roles ids
			//res= res.dropDuplicates(new String {});
			return res;
	}
	
	
	
	
	public Dataset<pojo.Buys> getBuysList(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.CustomerAttribute> client_condition){
			BuysServiceImpl buysService = this;
			OrderService orderService = new OrderServiceImpl();  
			CustomerService customerService = new CustomerServiceImpl();
			MutableBoolean order_refilter = new MutableBoolean(false);
			List<Dataset<Buys>> datasetsPOJO = new ArrayList<Dataset<Buys>>();
			boolean all_already_persisted = false;
			MutableBoolean client_refilter = new MutableBoolean(false);
			org.apache.spark.sql.Column joinCondition = null;
			// For role 'order' in reference 'clientRef'. A->B Scenario
			client_refilter = new MutableBoolean(false);
			Dataset<OrderTDO> orderTDOclientReforder = buysService.getOrderTDOListOrderInClientRefInOrderTableFromRelSchemaB(order_condition, order_refilter);
			Dataset<CustomerTDO> customerTDOclientRefclient = buysService.getCustomerTDOListClientInClientRefInOrderTableFromRelSchemaB(client_condition, client_refilter);
			
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
		
			Dataset<Buys> res_Order_clientRef = res_clientRef.map(
				new MapFunction<Row, Buys>() {
					public Buys call(Row r) {
						Buys res = new Buys();
						Order A = new Order();
						Customer B = new Customer();
						A.setId(r.getAs("id"));
						A.setOrderdate(r.getAs("orderdate"));
						A.setTotalprice(r.getAs("totalprice"));
						A.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("logEvents")));
		
						B.setId(r.getAs("Customer_id"));
						B.setFirstname(r.getAs("Customer_firstname"));
						B.setLastname(r.getAs("Customer_lastname"));
						B.setGender(r.getAs("Customer_gender"));
						B.setBirthday(r.getAs("Customer_birthday"));
						B.setCreationDate(r.getAs("Customer_creationDate"));
						B.setLocationip(r.getAs("Customer_locationip"));
						B.setBrowser(r.getAs("Customer_browser"));
						B.setLogEvents((ArrayList<String>) ScalaUtil.javaList(r.getAs("Customer_logEvents")));
						
						res.setOrder(A);
						res.setClient(B);
						return res;
					}
		
				},Encoders.bean(Buys.class)
			);
		
			datasetsPOJO.add(res_Order_clientRef);
		
			
			Dataset<Buys> res_buys_order;
			Dataset<Order> res_Order;
			// Role 'client' mapped to EmbeddedObject 'orders' 'Order' containing 'Customer' 
			client_refilter = new MutableBoolean(false);
			res_buys_order = buysService.getBuysListInmongoSchemaBuserColorders(client_condition, order_condition, client_refilter, order_refilter);
			
			datasetsPOJO.add(res_buys_order);
			
			
			//Join datasets or return 
			Dataset<Buys> res = fullOuterJoinsBuys(datasetsPOJO);
			if(res == null)
				return null;
		
			Dataset<Order> lonelyOrder = null;
			Dataset<Customer> lonelyClient = null;
			
		
		
			
			if(order_refilter.booleanValue() || client_refilter.booleanValue())
				res = res.filter((FilterFunction<Buys>) r -> (order_condition == null || order_condition.evaluate(r.getOrder())) && (client_condition == null || client_condition.evaluate(r.getClient())));
			
		
			return res;
		
		}
	
	public Dataset<pojo.Buys> getBuysListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		return getBuysList(order_condition, null);
	}
	
	public pojo.Buys getBuysByOrder(pojo.Order order) {
		conditions.Condition<conditions.OrderAttribute> cond = null;
		cond = conditions.Condition.simple(conditions.OrderAttribute.id, conditions.Operator.EQUALS, order.getId());
		Dataset<pojo.Buys> res = getBuysListByOrderCondition(cond);
		List<pojo.Buys> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<pojo.Buys> getBuysListByClientCondition(
		conditions.Condition<conditions.CustomerAttribute> client_condition
	){
		return getBuysList(null, client_condition);
	}
	
	public Dataset<pojo.Buys> getBuysListByClient(pojo.Customer client) {
		conditions.Condition<conditions.CustomerAttribute> cond = null;
		cond = conditions.Condition.simple(conditions.CustomerAttribute.id, conditions.Operator.EQUALS, client.getId());
		Dataset<pojo.Buys> res = getBuysListByClientCondition(cond);
	return res;
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
