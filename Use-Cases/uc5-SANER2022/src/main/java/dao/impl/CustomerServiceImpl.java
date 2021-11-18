package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.time.LocalDate;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Customer;
import conditions.*;
import dao.services.CustomerService;
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


public class CustomerServiceImpl extends CustomerService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CustomerServiceImpl.class);
	
	
	
	
	
	public static String getBSONMatchQueryInUserColFromMongoModelB(Condition<CustomerAttribute> condition, MutableBoolean refilterFlag) {	
		String res = null;	
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				CustomerAttribute attr = ((SimpleCondition<CustomerAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<CustomerAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<CustomerAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == CustomerAttribute.id ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "id': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == CustomerAttribute.firstname ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "firstName': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == CustomerAttribute.lastname ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "lastName': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == CustomerAttribute.gender ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "gender': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == CustomerAttribute.birthday ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "birthday': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == CustomerAttribute.creationDate ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "creationDate': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == CustomerAttribute.locationip ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "locationIP': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == CustomerAttribute.browser ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "browserUsed': {" + mongoOp + ": " + preparedValue + "}";
	
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
	
	public Dataset<Customer> getCustomerListInUserColFromMongoModelB(conditions.Condition<conditions.CustomerAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = CustomerServiceImpl.getBSONMatchQueryInUserColFromMongoModelB(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mongoModelB", "userCol", bsonQuery);
	
		Dataset<Customer> res = dataset.flatMap((FlatMapFunction<Row, Customer>) r -> {
				List<Customer> list_res = new ArrayList<Customer>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				Customer customer1 = new Customer();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Customer.id for field id			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("id")) {
						if(nestedRow.getAs("id")==null)
							customer1.setId(null);
						else{
							customer1.setId(Util.getStringValue(nestedRow.getAs("id")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.firstname for field firstName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("firstName")) {
						if(nestedRow.getAs("firstName")==null)
							customer1.setFirstname(null);
						else{
							customer1.setFirstname(Util.getStringValue(nestedRow.getAs("firstName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.lastname for field lastName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("lastName")) {
						if(nestedRow.getAs("lastName")==null)
							customer1.setLastname(null);
						else{
							customer1.setLastname(Util.getStringValue(nestedRow.getAs("lastName")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.gender for field gender			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("gender")) {
						if(nestedRow.getAs("gender")==null)
							customer1.setGender(null);
						else{
							customer1.setGender(Util.getStringValue(nestedRow.getAs("gender")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.birthday for field birthday			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("birthday")) {
						if(nestedRow.getAs("birthday")==null)
							customer1.setBirthday(null);
						else{
							customer1.setBirthday(Util.getLocalDateValue(nestedRow.getAs("birthday")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.creationDate for field creationDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("creationDate")) {
						if(nestedRow.getAs("creationDate")==null)
							customer1.setCreationDate(null);
						else{
							customer1.setCreationDate(Util.getLocalDateValue(nestedRow.getAs("creationDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.locationip for field locationIP			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("locationIP")) {
						if(nestedRow.getAs("locationIP")==null)
							customer1.setLocationip(null);
						else{
							customer1.setLocationip(Util.getStringValue(nestedRow.getAs("locationIP")));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.browser for field browserUsed			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("browserUsed")) {
						if(nestedRow.getAs("browserUsed")==null)
							customer1.setBrowser(null);
						else{
							customer1.setBrowser(Util.getStringValue(nestedRow.getAs("browserUsed")));
							toAdd1 = true;					
							}
					}
					if(toAdd1) {
						list_res.add(customer1);
						addedInList = true;
					} 
					
				
				return list_res.iterator();
	
		}, Encoders.bean(Customer.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
		
	}
	
	
	
	
	
	
	public Dataset<Customer> getReviewerListInFeedback(conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,conditions.Condition<conditions.CustomerAttribute> reviewer_condition, conditions.Condition<conditions.FeedbackAttribute> feedback_condition)		{
		MutableBoolean reviewer_refilter = new MutableBoolean(false);
		List<Dataset<Customer>> datasetsPOJO = new ArrayList<Dataset<Customer>>();
		Dataset<Product> all = null;
		boolean all_already_persisted = false;
		MutableBoolean reviewedProduct_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		
		Dataset<Feedback> res_feedback_reviewer;
		Dataset<Customer> res_Customer;
		
		
		//Join datasets or return 
		Dataset<Customer> res = fullOuterJoinsCustomer(datasetsPOJO);
		if(res == null)
			return null;
	
		List<Dataset<Customer>> lonelyCustomerList = new ArrayList<Dataset<Customer>>();
		lonelyCustomerList.add(getCustomerListInUserColFromMongoModelB(reviewer_condition, new MutableBoolean(false)));
		Dataset<Customer> lonelyCustomer = fullOuterJoinsCustomer(lonelyCustomerList);
		if(lonelyCustomer != null) {
			res = fullLeftOuterJoinsCustomer(Arrays.asList(res, lonelyCustomer));
		}
		if(reviewer_refilter.booleanValue())
			res = res.filter((FilterFunction<Customer>) r -> reviewer_condition == null || reviewer_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Customer> getClientListInBuys(conditions.Condition<conditions.OrderAttribute> order_condition,conditions.Condition<conditions.CustomerAttribute> client_condition)		{
		MutableBoolean client_refilter = new MutableBoolean(false);
		List<Dataset<Customer>> datasetsPOJO = new ArrayList<Dataset<Customer>>();
		Dataset<Order> all = null;
		boolean all_already_persisted = false;
		MutableBoolean order_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		order_refilter = new MutableBoolean(false);
		// For role 'order' in reference 'clientRef'  B->A Scenario
		Dataset<OrderTDO> orderTDOclientReforder = buysService.getOrderTDOListOrderInClientRefInOrderTableFromRelSchemaB(order_condition, order_refilter);
		Dataset<CustomerTDO> customerTDOclientRefclient = buysService.getCustomerTDOListClientInClientRefInOrderTableFromRelSchemaB(client_condition, client_refilter);
		if(order_refilter.booleanValue()) {
			if(all == null)
				all = new OrderServiceImpl().getOrderList(order_condition);
			joinCondition = null;
			joinCondition = orderTDOclientReforder.col("id").equalTo(all.col("id"));
			if(joinCondition == null)
				orderTDOclientReforder = orderTDOclientReforder.as("A").join(all).select("A.*").as(Encoders.bean(OrderTDO.class));
			else
				orderTDOclientReforder = orderTDOclientReforder.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(OrderTDO.class));
		}
		Dataset<Row> res_clientRef = 
			customerTDOclientRefclient.join(orderTDOclientReforder
				.withColumnRenamed("id", "Order_id")
				.withColumnRenamed("orderdate", "Order_orderdate")
				.withColumnRenamed("totalprice", "Order_totalprice")
				.withColumnRenamed("logEvents", "Order_logEvents"),
				customerTDOclientRefclient.col("relSchemaB_orderTable_clientRef_id").equalTo(orderTDOclientReforder.col("relSchemaB_orderTable_clientRef_customerId")));
		Dataset<Customer> res_Customer_clientRef = res_clientRef.select( "id", "firstname", "lastname", "gender", "birthday", "creationDate", "locationip", "browser", "logEvents").as(Encoders.bean(Customer.class));
		res_Customer_clientRef = res_Customer_clientRef.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Customer_clientRef);
		
		Dataset<Buys> res_buys_client;
		Dataset<Customer> res_Customer;
		// Role 'client' mapped to EmbeddedObject 'orders' - 'Order' containing 'Customer'
		order_refilter = new MutableBoolean(false);
		res_buys_client = buysService.getBuysListInmongoSchemaBuserColorders(client_condition, order_condition, client_refilter, order_refilter);
		if(order_refilter.booleanValue()) {
			if(all == null)
				all = new OrderServiceImpl().getOrderList(order_condition);
			joinCondition = null;
			joinCondition = res_buys_client.col("order.id").equalTo(all.col("id"));
			if(joinCondition == null)
				res_Customer = res_buys_client.join(all).select("client.*").as(Encoders.bean(Customer.class));
			else
				res_Customer = res_buys_client.join(all, joinCondition).select("client.*").as(Encoders.bean(Customer.class));
		
		} else
			res_Customer = res_buys_client.map((MapFunction<Buys,Customer>) r -> r.getClient(), Encoders.bean(Customer.class));
		res_Customer = res_Customer.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Customer);
		
		
		//Join datasets or return 
		Dataset<Customer> res = fullOuterJoinsCustomer(datasetsPOJO);
		if(res == null)
			return null;
	
		if(client_refilter.booleanValue())
			res = res.filter((FilterFunction<Customer>) r -> client_condition == null || client_condition.evaluate(r));
		
	
		return res;
		}
	
	
	public boolean insertCustomer(Customer customer){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertCustomerInUserColFromMongoModelB(customer) || inserted ;
		return inserted;
	}
	
	public boolean insertCustomerInUserColFromMongoModelB(Customer customer)	{
		Condition<CustomerAttribute> conditionID;
		String idvalue="";
		boolean entityExists=false;
		conditionID = Condition.simple(CustomerAttribute.id, Operator.EQUALS, customer.getId());
		idvalue+=customer.getId();
		Dataset res = getCustomerListInUserColFromMongoModelB(conditionID,new MutableBoolean(false));
		entityExists = res != null && !res.isEmpty();
				
		if(!entityExists){
		Bson filter = new Document();
		Bson updateOp;
		Document docuserCol_1 = new Document();
		docuserCol_1.append("id",customer.getId());
		docuserCol_1.append("firstName",customer.getFirstname());
		docuserCol_1.append("lastName",customer.getLastname());
		docuserCol_1.append("gender",customer.getGender());
		docuserCol_1.append("birthday",customer.getBirthday());
		docuserCol_1.append("creationDate",customer.getCreationDate());
		docuserCol_1.append("locationIP",customer.getLocationip());
		docuserCol_1.append("browserUsed",customer.getBrowser());
		
		filter = eq("id",customer.getId());
		updateOp = setOnInsert(docuserCol_1);
		DBConnectionMgr.upsertMany(filter, updateOp, "userCol", "mongoModelB");
			logger.info("Inserted [Customer] entity ID [{}] in [UserCol] in database [MongoModelB]", idvalue);
		}
		else
			logger.warn("[Customer] entity ID [{}] already present in [UserCol] in database [MongoModelB]", idvalue);
		return !entityExists;
	} 
	
	public void updateCustomerList(conditions.Condition<conditions.CustomerAttribute> condition, conditions.SetClause<conditions.CustomerAttribute> set){
		//TODO
	}
	
	public void updateCustomer(pojo.Customer customer) {
		//TODO using the id
		return;
	}
	public void updateReviewerListInFeedback(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		conditions.Condition<conditions.FeedbackAttribute> feedback,
		conditions.SetClause<conditions.CustomerAttribute> set
	){
		//TODO
	}
	
	public void updateReviewerListInFeedbackByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,
		conditions.SetClause<conditions.CustomerAttribute> set
	){
		updateReviewerListInFeedback(reviewedProduct_condition, null, null, set);
	}
	
	public void updateReviewerListInFeedbackByReviewedProduct(
		pojo.Product reviewedProduct,
		conditions.SetClause<conditions.CustomerAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateReviewerListInFeedbackByReviewerCondition(
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		conditions.SetClause<conditions.CustomerAttribute> set
	){
		updateReviewerListInFeedback(null, reviewer_condition, null, set);
	}
	public void updateReviewerListInFeedbackByFeedbackCondition(
		conditions.Condition<conditions.FeedbackAttribute> feedback_condition,
		conditions.SetClause<conditions.CustomerAttribute> set
	){
		updateReviewerListInFeedback(null, null, feedback_condition, set);
	}
	public void updateClientListInBuys(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.CustomerAttribute> client_condition,
		
		conditions.SetClause<conditions.CustomerAttribute> set
	){
		//TODO
	}
	
	public void updateClientListInBuysByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.CustomerAttribute> set
	){
		updateClientListInBuys(order_condition, null, set);
	}
	
	public void updateClientInBuysByOrder(
		pojo.Order order,
		conditions.SetClause<conditions.CustomerAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateClientListInBuysByClientCondition(
		conditions.Condition<conditions.CustomerAttribute> client_condition,
		conditions.SetClause<conditions.CustomerAttribute> set
	){
		updateClientListInBuys(null, client_condition, set);
	}
	
	
	public void deleteCustomerList(conditions.Condition<conditions.CustomerAttribute> condition){
		//TODO
	}
	
	public void deleteCustomer(pojo.Customer customer) {
		//TODO using the id
		return;
	}
	public void deleteReviewerListInFeedback(	
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,	
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		conditions.Condition<conditions.FeedbackAttribute> feedback){
			//TODO
		}
	
	public void deleteReviewerListInFeedbackByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition
	){
		deleteReviewerListInFeedback(reviewedProduct_condition, null, null);
	}
	
	public void deleteReviewerListInFeedbackByReviewedProduct(
		pojo.Product reviewedProduct 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteReviewerListInFeedbackByReviewerCondition(
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition
	){
		deleteReviewerListInFeedback(null, reviewer_condition, null);
	}
	public void deleteReviewerListInFeedbackByFeedbackCondition(
		conditions.Condition<conditions.FeedbackAttribute> feedback_condition
	){
		deleteReviewerListInFeedback(null, null, feedback_condition);
	}
	public void deleteClientListInBuys(	
		conditions.Condition<conditions.OrderAttribute> order_condition,	
		conditions.Condition<conditions.CustomerAttribute> client_condition){
			//TODO
		}
	
	public void deleteClientListInBuysByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteClientListInBuys(order_condition, null);
	}
	
	public void deleteClientInBuysByOrder(
		pojo.Order order 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteClientListInBuysByClientCondition(
		conditions.Condition<conditions.CustomerAttribute> client_condition
	){
		deleteClientListInBuys(null, client_condition);
	}
	
}
