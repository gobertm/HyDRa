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
	
	
	
	public static Pair<String, List<String>> getSQLWhereClauseInCustomerTableFromMysqlbench(Condition<CustomerAttribute> condition, MutableBoolean refilterFlag) {
		return getSQLWhereClauseInCustomerTableFromMysqlbenchWithTableAlias(condition, refilterFlag, "");
	}
	
	public static Pair<String, List<String>> getSQLWhereClauseInCustomerTableFromMysqlbenchWithTableAlias(Condition<CustomerAttribute> condition, MutableBoolean refilterFlag, String tableAlias) {
		String where = null;	
		List<String> preparedValues = new java.util.ArrayList<String>();
		if(condition != null) {
			
			if(condition instanceof SimpleCondition) {
				CustomerAttribute attr = ((SimpleCondition<CustomerAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<CustomerAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<CustomerAttribute>) condition).getValue();
				if(value != null) {
					boolean isConditionAttrEncountered = false;
					if(attr == CustomerAttribute.id ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "id " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(attr == CustomerAttribute.firstname ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "firstName " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(attr == CustomerAttribute.lastname ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "lastName " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(attr == CustomerAttribute.gender ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "gender " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(attr == CustomerAttribute.birthday ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "birthday " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(attr == CustomerAttribute.creationDate ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "creationDate " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(attr == CustomerAttribute.locationip ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "locationIP " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(attr == CustomerAttribute.browser ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "browserUsed " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						where = "1 = 1";
					}
				}
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInCustomerTableFromMysqlbench(((AndCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInCustomerTableFromMysqlbench(((AndCondition) condition).getRightCondition(), refilterFlag);
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
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInCustomerTableFromMysqlbench(((OrCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInCustomerTableFromMysqlbench(((OrCondition) condition).getRightCondition(), refilterFlag);
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
	
	
	public Dataset<Customer> getCustomerListInCustomerTableFromMysqlbench(conditions.Condition<conditions.CustomerAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = CustomerServiceImpl.getSQLWhereClauseInCustomerTableFromMysqlbench(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", "'" + Util.escapeQuote(preparedValue) + "'");
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("mysqlbench", "customerTable", where);
		
	
		Dataset<Customer> res = d.map((MapFunction<Row, Customer>) r -> {
					Customer customer_res = new Customer();
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
	
	
	
					return customer_res;
				}, Encoders.bean(Customer.class));
	
	
		return res;
		
	}
	
	
	// TODO get based on id(s). Ex:public Client getClientById(Long id)
	public Customer getCustomerById(String id){
		Condition cond;
		cond = Condition.simple(CustomerAttribute.id, conditions.Operator.EQUALS, id);
		Dataset<Customer> res = getCustomerList(cond);
		if(res!=null)
			return res.first();
		return null;
	}
	
	public Dataset<Customer> getCustomerListById(String id) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Customer> getCustomerListByFirstname(String firstname) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.firstname, conditions.Operator.EQUALS, firstname));
	}
	
	public Dataset<Customer> getCustomerListByLastname(String lastname) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.lastname, conditions.Operator.EQUALS, lastname));
	}
	
	public Dataset<Customer> getCustomerListByGender(String gender) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.gender, conditions.Operator.EQUALS, gender));
	}
	
	public Dataset<Customer> getCustomerListByBirthday(LocalDate birthday) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.birthday, conditions.Operator.EQUALS, birthday));
	}
	
	public Dataset<Customer> getCustomerListByCreationDate(LocalDate creationDate) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.creationDate, conditions.Operator.EQUALS, creationDate));
	}
	
	public Dataset<Customer> getCustomerListByLocationip(String locationip) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.locationip, conditions.Operator.EQUALS, locationip));
	}
	
	public Dataset<Customer> getCustomerListByBrowser(String browser) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.browser, conditions.Operator.EQUALS, browser));
	}
	
	
	
	
	public Dataset<Customer> getClientListInBuys(conditions.Condition<conditions.OrderAttribute> order_condition,conditions.Condition<conditions.CustomerAttribute> client_condition)		{
		MutableBoolean client_refilter = new MutableBoolean(false);
		List<Dataset<Customer>> datasetsPOJO = new ArrayList<Dataset<Customer>>();
		Dataset<Order> all = null;
		boolean all_already_persisted = false;
		MutableBoolean order_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		order_refilter = new MutableBoolean(false);
		// For role 'order' in reference 'customer'  B->A Scenario
		Dataset<OrderTDO> orderTDOcustomerorder = buysService.getOrderTDOListOrderInCustomerInOrdersColFromDocSchema(order_condition, order_refilter);
		Dataset<CustomerTDO> customerTDOcustomerclient = buysService.getCustomerTDOListClientInCustomerInOrdersColFromDocSchema(client_condition, client_refilter);
		if(order_refilter.booleanValue()) {
			if(all == null)
				all = new OrderServiceImpl().getOrderList(order_condition);
			joinCondition = null;
			joinCondition = orderTDOcustomerorder.col("id").equalTo(all.col("id"));
			if(joinCondition == null)
				orderTDOcustomerorder = orderTDOcustomerorder.as("A").join(all).select("A.*").as(Encoders.bean(OrderTDO.class));
			else
				orderTDOcustomerorder = orderTDOcustomerorder.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(OrderTDO.class));
		}
		Dataset<Row> res_customer = 
			customerTDOcustomerclient.join(orderTDOcustomerorder
				.withColumnRenamed("id", "Order_id")
				.withColumnRenamed("orderdate", "Order_orderdate")
				.withColumnRenamed("totalprice", "Order_totalprice")
				.withColumnRenamed("logEvents", "Order_logEvents"),
				customerTDOcustomerclient.col("docSchema_ordersCol_customer_id").equalTo(orderTDOcustomerorder.col("docSchema_ordersCol_customer_PersonId")));
		Dataset<Customer> res_Customer_customer = res_customer.select( "id", "firstname", "lastname", "gender", "birthday", "creationDate", "locationip", "browser", "logEvents").as(Encoders.bean(Customer.class));
		res_Customer_customer = res_Customer_customer.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Customer_customer);
		
		Dataset<Buys> res_buys_client;
		Dataset<Customer> res_Customer;
		
		
		//Join datasets or return 
		Dataset<Customer> res = fullOuterJoinsCustomer(datasetsPOJO);
		if(res == null)
			return null;
	
		if(client_refilter.booleanValue())
			res = res.filter((FilterFunction<Customer>) r -> client_condition == null || client_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Customer> getClientListInBuysByOrderCondition(conditions.Condition<conditions.OrderAttribute> order_condition){
		return getClientListInBuys(order_condition, null);
	}
	
	public Customer getClientInBuysByOrder(pojo.Order order){
		if(order == null)
			return null;
	
		Condition c;
		c=Condition.simple(OrderAttribute.id,Operator.EQUALS, order.getId());
		Dataset<Customer> res = getClientListInBuysByOrderCondition(c);
		return !res.isEmpty()?res.first():null;
	}
	
	public Dataset<Customer> getClientListInBuysByClientCondition(conditions.Condition<conditions.CustomerAttribute> client_condition){
		return getClientListInBuys(null, client_condition);
	}
	public Dataset<Customer> getReviewerListInWrite(conditions.Condition<conditions.FeedbackAttribute> review_condition,conditions.Condition<conditions.CustomerAttribute> reviewer_condition)		{
		MutableBoolean reviewer_refilter = new MutableBoolean(false);
		List<Dataset<Customer>> datasetsPOJO = new ArrayList<Dataset<Customer>>();
		Dataset<Feedback> all = null;
		boolean all_already_persisted = false;
		MutableBoolean review_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		review_refilter = new MutableBoolean(false);
		// For role 'review' in reference 'customer1'  B->A Scenario
		Dataset<FeedbackTDO> feedbackTDOcustomer1review = writeService.getFeedbackTDOListReviewInCustomer1InFeedbackFromKvSchema(review_condition, review_refilter);
		Dataset<CustomerTDO> customerTDOcustomer1reviewer = writeService.getCustomerTDOListReviewerInCustomer1InFeedbackFromKvSchema(reviewer_condition, reviewer_refilter);
		if(review_refilter.booleanValue()) {
			if(all == null)
				all = new FeedbackServiceImpl().getFeedbackList(review_condition);
			joinCondition = null;
			if(joinCondition == null)
				feedbackTDOcustomer1review = feedbackTDOcustomer1review.as("A").join(all).select("A.*").as(Encoders.bean(FeedbackTDO.class));
			else
				feedbackTDOcustomer1review = feedbackTDOcustomer1review.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(FeedbackTDO.class));
		}
		Dataset<Row> res_customer1 = 
			customerTDOcustomer1reviewer.join(feedbackTDOcustomer1review
				.withColumnRenamed("rate", "Feedback_rate")
				.withColumnRenamed("content", "Feedback_content")
				.withColumnRenamed("product", "Feedback_product")
				.withColumnRenamed("customer", "Feedback_customer")
				.withColumnRenamed("logEvents", "Feedback_logEvents"),
				customerTDOcustomer1reviewer.col("kvSchema_feedback_customer1_id").equalTo(feedbackTDOcustomer1review.col("kvSchema_feedback_customer1_customerid")));
		Dataset<Customer> res_Customer_customer1 = res_customer1.select( "id", "firstname", "lastname", "gender", "birthday", "creationDate", "locationip", "browser", "logEvents").as(Encoders.bean(Customer.class));
		res_Customer_customer1 = res_Customer_customer1.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Customer_customer1);
		
		Dataset<Write> res_write_reviewer;
		Dataset<Customer> res_Customer;
		
		
		//Join datasets or return 
		Dataset<Customer> res = fullOuterJoinsCustomer(datasetsPOJO);
		if(res == null)
			return null;
	
		if(reviewer_refilter.booleanValue())
			res = res.filter((FilterFunction<Customer>) r -> reviewer_condition == null || reviewer_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Customer> getReviewerListInWriteByReviewCondition(conditions.Condition<conditions.FeedbackAttribute> review_condition){
		return getReviewerListInWrite(review_condition, null);
	}
	
	public Dataset<Customer> getReviewerListInWriteByReview(pojo.Feedback review){
		if(review == null)
			return null;
	
		Condition c;
		c=null;
		Dataset<Customer> res = getReviewerListInWriteByReviewCondition(c);
		return res;
	}
	
	public Dataset<Customer> getReviewerListInWriteByReviewerCondition(conditions.Condition<conditions.CustomerAttribute> reviewer_condition){
		return getReviewerListInWrite(null, reviewer_condition);
	}
	
	
	public boolean insertCustomer(Customer customer){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertCustomerInCustomerTableFromMysqlbench(customer) || inserted ;
		return inserted;
	}
	
	public boolean insertCustomerInCustomerTableFromMysqlbench(Customer customer)	{
		Condition<CustomerAttribute> conditionID;
		String idvalue="";
		boolean entityExists=false;
		conditionID = Condition.simple(CustomerAttribute.id, Operator.EQUALS, customer.getId());
		idvalue+=customer.getId();
		Dataset res = getCustomerListInCustomerTableFromMysqlbench(conditionID,new MutableBoolean(false));
		entityExists = res != null && !res.isEmpty();
				
		if(!entityExists){
		List<String> columns = new ArrayList<>();
		List<Object> values = new ArrayList<>();	
		columns.add("id");
		values.add(customer.getId());
		columns.add("firstName");
		values.add(customer.getFirstname());
		columns.add("lastName");
		values.add(customer.getLastname());
		columns.add("gender");
		values.add(customer.getGender());
		columns.add("birthday");
		values.add(customer.getBirthday());
		columns.add("creationDate");
		values.add(customer.getCreationDate());
		columns.add("locationIP");
		values.add(customer.getLocationip());
		columns.add("browserUsed");
		values.add(customer.getBrowser());
		DBConnectionMgr.insertInTable(columns, Arrays.asList(values), "customerTable", "mysqlbench");
			logger.info("Inserted [Customer] entity ID [{}] in [CustomerTable] in database [Mysqlbench]", idvalue);
		}
		else
			logger.warn("[Customer] entity ID [{}] already present in [CustomerTable] in database [Mysqlbench]", idvalue);
		return !entityExists;
	} 
	
	public void updateCustomerList(conditions.Condition<conditions.CustomerAttribute> condition, conditions.SetClause<conditions.CustomerAttribute> set){
		//TODO
	}
	
	public void updateCustomer(pojo.Customer customer) {
		//TODO using the id
		return;
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
	public void updateReviewerListInWrite(
		conditions.Condition<conditions.FeedbackAttribute> review_condition,
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		
		conditions.SetClause<conditions.CustomerAttribute> set
	){
		//TODO
	}
	
	public void updateReviewerListInWriteByReviewCondition(
		conditions.Condition<conditions.FeedbackAttribute> review_condition,
		conditions.SetClause<conditions.CustomerAttribute> set
	){
		updateReviewerListInWrite(review_condition, null, set);
	}
	
	public void updateReviewerListInWriteByReview(
		pojo.Feedback review,
		conditions.SetClause<conditions.CustomerAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateReviewerListInWriteByReviewerCondition(
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		conditions.SetClause<conditions.CustomerAttribute> set
	){
		updateReviewerListInWrite(null, reviewer_condition, set);
	}
	
	
	public void deleteCustomerList(conditions.Condition<conditions.CustomerAttribute> condition){
		//TODO
	}
	
	public void deleteCustomer(pojo.Customer customer) {
		//TODO using the id
		return;
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
	public void deleteReviewerListInWrite(	
		conditions.Condition<conditions.FeedbackAttribute> review_condition,	
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition){
			//TODO
		}
	
	public void deleteReviewerListInWriteByReviewCondition(
		conditions.Condition<conditions.FeedbackAttribute> review_condition
	){
		deleteReviewerListInWrite(review_condition, null);
	}
	
	public void deleteReviewerListInWriteByReview(
		pojo.Feedback review 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteReviewerListInWriteByReviewerCondition(
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition
	){
		deleteReviewerListInWrite(null, reviewer_condition);
	}
	
}
