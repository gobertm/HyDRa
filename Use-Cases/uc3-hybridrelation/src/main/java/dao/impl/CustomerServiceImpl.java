package dao.impl;

import java.util.Arrays;
import java.util.List;
import pojo.Customer;
import conditions.*;
import dao.services.CustomerService;
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

public class CustomerServiceImpl extends CustomerService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CustomerServiceImpl.class);
	
	
	
	public static String getBSONMatchQueryInClientCollectionFromMongo(Condition<CustomerAttribute> condition, MutableBoolean refilterFlag) {	
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
					if(attr == CustomerAttribute.firstName ) {
						isConditionAttrEncountered = true;
					
					String preparedValue = "@VAR@ @OTHERVAR@";
					boolean like_op = false;
					boolean not_like = false; 
					if(op == Operator.EQUALS && valueString != null && preparedValue.contains("@OTHERVAR@")) {
						//ex: @@VAR@@' '@@OTHERVAR@@"=> more than one vars in LongField: we shall use regex
						like_op = true;
						preparedValue = "@VAR@ @OTHERVAR@";
					} else {
						if(op == Operator.NOT_EQUALS && valueString != null && preparedValue.contains("@OTHERVAR@")) {
							//ex: @@VAR@@' '@@OTHERVAR@@"=> more than one vars in LongField: we shall use regex
							not_like = true;
							like_op = true;
							preparedValue = "@VAR@ @OTHERVAR@";
						}
					}
					if(op == Operator.CONTAINS && valueString != null) {
						like_op = true;
						preparedValue = "@VAR@ @OTHERVAR@";
						preparedValue = preparedValue.replaceAll("@VAR@", ".*@VAR@.*");
					}
						
					if(like_op)
						valueString = Util.escapeReservedRegexMongo(valueString);
					preparedValue = preparedValue.replaceAll("@VAR@", valueString).replaceAll("@OTHERVAR@", ".*");
					
					if(valueString.equals(preparedValue)) // 5 <=> 5, the preparedValue is the same type as the original value
						preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
					else // 5 => 5*, the preparedValue became a string
						preparedValue = Util.getDelimitedMongoValue(String.class, preparedValue);
						
	
					String mongoOp = (like_op) ? "$regex" : op.getMongoDBOperator();
					//if not_like = true then we need to complement/negate the given regex
					res = "fullName': {" + (!not_like ? (mongoOp + ": " + preparedValue) : ("$not: {" + mongoOp + ": " + preparedValue + "}")) + "}";
	
					res = "'" + res;
					}
					if(attr == CustomerAttribute.lastName ) {
						isConditionAttrEncountered = true;
					
					String preparedValue = "@OTHERVAR@ @VAR@";
					boolean like_op = false;
					boolean not_like = false; 
					if(op == Operator.EQUALS && valueString != null && preparedValue.contains("@OTHERVAR@")) {
						//ex: @@VAR@@' '@@OTHERVAR@@"=> more than one vars in LongField: we shall use regex
						like_op = true;
						preparedValue = "@OTHERVAR@ @VAR@";
					} else {
						if(op == Operator.NOT_EQUALS && valueString != null && preparedValue.contains("@OTHERVAR@")) {
							//ex: @@VAR@@' '@@OTHERVAR@@"=> more than one vars in LongField: we shall use regex
							not_like = true;
							like_op = true;
							preparedValue = "@OTHERVAR@ @VAR@";
						}
					}
					if(op == Operator.CONTAINS && valueString != null) {
						like_op = true;
						preparedValue = "@OTHERVAR@ @VAR@";
						preparedValue = preparedValue.replaceAll("@VAR@", ".*@VAR@.*");
					}
						
					if(like_op)
						valueString = Util.escapeReservedRegexMongo(valueString);
					preparedValue = preparedValue.replaceAll("@VAR@", valueString).replaceAll("@OTHERVAR@", ".*");
					
					if(valueString.equals(preparedValue)) // 5 <=> 5, the preparedValue is the same type as the original value
						preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
					else // 5 => 5*, the preparedValue became a string
						preparedValue = Util.getDelimitedMongoValue(String.class, preparedValue);
						
	
					String mongoOp = (like_op) ? "$regex" : op.getMongoDBOperator();
					//if not_like = true then we need to complement/negate the given regex
					res = "fullName': {" + (!not_like ? (mongoOp + ": " + preparedValue) : ("$not: {" + mongoOp + ": " + preparedValue + "}")) + "}";
	
					res = "'" + res;
					}
					if(attr == CustomerAttribute.address ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "postalAddress': {" + mongoOp + ": " + preparedValue + "}";
	
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
	
	public Dataset<Customer> getCustomerListInClientCollectionFromMongo(conditions.Condition<conditions.CustomerAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = CustomerServiceImpl.getBSONMatchQueryInClientCollectionFromMongo(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mongo", "ClientCollection", bsonQuery);
	
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
							customer1.setId((Integer) nestedRow.getAs("id"));
							toAdd1 = true;					
							}
					}
					// 	attribute Customer.firstName for field fullName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("fullName")) {
						regex = "(.*)( )(.*)";
						groupIndex = 1;
						if(groupIndex == null) {
							throw new Exception("Cannot retrieve value for Customer.firstName attribute stored in db mongo. Probably due to an ambiguous regex.");
						}
						value = nestedRow.getAs("fullName");
						p = Pattern.compile(regex);
						m = p.matcher(value);
						matches = m.find();
						if(matches) {
							String firstName = m.group(groupIndex.intValue());
							customer1.setFirstName(firstName == null ? null : firstName);
							toAdd1 = true;
						} else {
							throw new Exception("Cannot retrieve value for Customer.firstName attribute stored in db mongo. Probably due to an ambiguous regex.");
						}
					}
					// 	attribute Customer.lastName for field fullName			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("fullName")) {
						regex = "(.*)( )(.*)";
						groupIndex = 3;
						if(groupIndex == null) {
							throw new Exception("Cannot retrieve value for Customer.lastName attribute stored in db mongo. Probably due to an ambiguous regex.");
						}
						value = nestedRow.getAs("fullName");
						p = Pattern.compile(regex);
						m = p.matcher(value);
						matches = m.find();
						if(matches) {
							String lastName = m.group(groupIndex.intValue());
							customer1.setLastName(lastName == null ? null : lastName);
							toAdd1 = true;
						} else {
							throw new Exception("Cannot retrieve value for Customer.lastName attribute stored in db mongo. Probably due to an ambiguous regex.");
						}
					}
					// 	attribute Customer.address for field postalAddress			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("postalAddress")) {
						if(nestedRow.getAs("postalAddress")==null)
							customer1.setAddress(null);
						else{
							customer1.setAddress((String) nestedRow.getAs("postalAddress"));
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
	
	
	// TODO get based on id(s). Ex:public Client getClientById(Long id)
	
	public Dataset<Customer> getCustomerListById(Integer id) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Customer> getCustomerListByFirstName(String firstName) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.firstName, conditions.Operator.EQUALS, firstName));
	}
	
	public Dataset<Customer> getCustomerListByLastName(String lastName) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.lastName, conditions.Operator.EQUALS, lastName));
	}
	
	public Dataset<Customer> getCustomerListByAddress(String address) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.address, conditions.Operator.EQUALS, address));
	}
	
	
	
	
	public Dataset<Customer> getBuyerListInPlaces(conditions.Condition<conditions.CustomerAttribute> buyer_condition,conditions.Condition<conditions.OrderAttribute> order_condition)		{
		MutableBoolean buyer_refilter = new MutableBoolean(false);
		List<Dataset<Customer>> datasetsPOJO = new ArrayList<Dataset<Customer>>();
		Dataset<Order> all = new OrderServiceImpl().getOrderList(order_condition);
		boolean all_already_persisted = false;
		MutableBoolean order_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		
		// Role 'buyer' mapped to EmbeddedObject 'orders' - 'Order' containing 'Customer'
		order_refilter = new MutableBoolean(false);
		Dataset<Places> res_places_buyer;
		res_places_buyer = placesService.getPlacesListInmongoSchemaClientCollectionorders(buyer_condition, order_condition, buyer_refilter, order_refilter);
		Dataset<Customer> res_Customer;
		if(order_refilter.booleanValue()) {
			joinCondition = null;
			joinCondition = res_places_buyer.col("order.id").equalTo(all.col("id"));
			if(joinCondition == null)
				res_Customer = res_places_buyer.join(all).select("buyer.*").as(Encoders.bean(Customer.class));
			else
				res_Customer = res_places_buyer.join(all, joinCondition).select("buyer.*").as(Encoders.bean(Customer.class));
		
		} else
			res_Customer = res_places_buyer.map((MapFunction<Places,Customer>) r -> r.getBuyer(), Encoders.bean(Customer.class));
		res_Customer = res_Customer.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Customer);
		
		
		//Join datasets or return 
		Dataset<Customer> res = fullOuterJoinsCustomer(datasetsPOJO);
		if(res == null)
			return null;
	
		if(buyer_refilter.booleanValue())
			res = res.filter((FilterFunction<Customer>) r -> buyer_condition == null || buyer_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Customer> getBuyerListInPlacesByBuyerCondition(conditions.Condition<conditions.CustomerAttribute> buyer_condition){
		return getBuyerListInPlaces(buyer_condition, null);
	}
	public Dataset<Customer> getBuyerListInPlacesByOrderCondition(conditions.Condition<conditions.OrderAttribute> order_condition){
		return getBuyerListInPlaces(null, order_condition);
	}
	
	public Customer getBuyerInPlacesByOrder(pojo.Order order){
		if(order == null)
			return null;
	
		Condition c;
		c=Condition.simple(OrderAttribute.id,Operator.EQUALS, order.getId());
		Dataset<Customer> res = getBuyerListInPlacesByOrderCondition(c);
		return res.first();
	}
	
	
	public void insertCustomerAndLinkedItems(Customer customer){
		//TODO
	}
	public void insertCustomer(Customer customer){
		// Insert into all mapped AbstractPhysicalStructure 
			insertCustomerInClientCollectionFromMongo(customer);
	}
	
	public void insertCustomerInClientCollectionFromMongo(Customer customer){
		//Read mapping rules and find attributes of the POJO that are mapped to the corresponding AbstractPhysicalStructure
		// Insert in MongoDB
	}
	
	public void updateCustomerList(conditions.Condition<conditions.CustomerAttribute> condition, conditions.SetClause<conditions.CustomerAttribute> set){
		//TODO
	}
	
	public void updateCustomer(pojo.Customer customer) {
		//TODO using the id
		return;
	}
	public void updateBuyerListInPlaces(
		conditions.Condition<conditions.CustomerAttribute> buyer_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition,
		
		conditions.SetClause<conditions.CustomerAttribute> set
	){
		//TODO
	}
	
	public void updateBuyerListInPlacesByBuyerCondition(
		conditions.Condition<conditions.CustomerAttribute> buyer_condition,
		conditions.SetClause<conditions.CustomerAttribute> set
	){
		updateBuyerListInPlaces(buyer_condition, null, set);
	}
	public void updateBuyerListInPlacesByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.CustomerAttribute> set
	){
		updateBuyerListInPlaces(null, order_condition, set);
	}
	
	public void updateBuyerInPlacesByOrder(
		pojo.Order order,
		conditions.SetClause<conditions.CustomerAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	
	
	public void deleteCustomerList(conditions.Condition<conditions.CustomerAttribute> condition){
		//TODO
	}
	
	public void deleteCustomer(pojo.Customer customer) {
		//TODO using the id
		return;
	}
	public void deleteBuyerListInPlaces(	
		conditions.Condition<conditions.CustomerAttribute> buyer_condition,	
		conditions.Condition<conditions.OrderAttribute> order_condition){
			//TODO
		}
	
	public void deleteBuyerListInPlacesByBuyerCondition(
		conditions.Condition<conditions.CustomerAttribute> buyer_condition
	){
		deleteBuyerListInPlaces(buyer_condition, null);
	}
	public void deleteBuyerListInPlacesByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteBuyerListInPlaces(null, order_condition);
	}
	
	public void deleteBuyerInPlacesByOrder(
		pojo.Order order 
	){
		//TODO get id in condition
		return;	
	}
	
	
}
