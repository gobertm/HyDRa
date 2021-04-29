package dao.impl;

import java.util.Arrays;
import org.apache.spark.sql.Dataset;
import conditions.Condition;
import conditions.AndCondition;
import conditions.OrCondition;
import conditions.SimpleCondition;
import conditions.PlacesAttribute;
import conditions.Operator;
import pojo.Places;
import tdo.CustomerTDO;
import tdo.PlacesTDO;
import pojo.Customer;
import conditions.CustomerAttribute;
import tdo.OrderTDO;
import tdo.PlacesTDO;
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


public class PlacesServiceImpl extends dao.services.PlacesService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlacesServiceImpl.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	
	// method accessing the embedded object orders mapped to role buyer
	public Dataset<pojo.Places> getPlacesListInmongoSchemaClientCollectionorders(Condition<CustomerAttribute> buyer_condition, Condition<OrderAttribute> order_condition, MutableBoolean buyer_refilter, MutableBoolean order_refilter){	
			List<String> bsons = new ArrayList<String>();
			String bson = null;
			bson = CustomerServiceImpl.getBSONMatchQueryInClientCollectionFromMongo(buyer_condition ,buyer_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
			bson = OrderServiceImpl.getBSONMatchQueryInClientCollectionFromMongo(order_condition ,order_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
		
			String bsonQuery = bsons.size() == 0 ? null : "{$match: { $and: [" + String.join(",", bsons) + "] }}";
		
			Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mongo", "ClientCollection", bsonQuery);
		
			Dataset<Places> res = dataset.flatMap((FlatMapFunction<Row, Places>) r -> {
					List<Places> list_res = new ArrayList<Places>();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					Row nestedRow = null;
		
					boolean addedInList = false;
					Row r1 = r;
					Places places1 = new Places();
					places1.setBuyer(new Customer());
					places1.setOrder(new Order());
					
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Customer.id for field id			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("id")) {
						if(nestedRow.getAs("id")==null)
							places1.getBuyer().setId(null);
						else{
							places1.getBuyer().setId((Integer)nestedRow.getAs("id"));
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
							places1.getBuyer().setFirstName(firstName == null ? null : firstName);
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
							places1.getBuyer().setLastName(lastName == null ? null : lastName);
							toAdd1 = true;
						} else {
							throw new Exception("Cannot retrieve value for Customer.lastName attribute stored in db mongo. Probably due to an ambiguous regex.");
						}
					}
					// 	attribute Customer.address for field postalAddress			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("postalAddress")) {
						if(nestedRow.getAs("postalAddress")==null)
							places1.getBuyer().setAddress(null);
						else{
							places1.getBuyer().setAddress((String)nestedRow.getAs("postalAddress"));
							toAdd1 = true;					
							}
					}
					array1 = r1.getAs("orders");
					if(array1!= null) {
						for (int i2 = 0; i2 < array1.size(); i2++){
							Row r2 = (Row) array1.apply(i2);
							Places places2 = (Places) places1.clone();
							boolean toAdd2  = false;
							WrappedArray array2  = null;
							// 	attribute Order.id for field orderId			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("orderId")) {
								if(nestedRow.getAs("orderId")==null)
									places2.getOrder().setId(null);
								else{
									places2.getOrder().setId((Integer)nestedRow.getAs("orderId"));
									toAdd2 = true;					
									}
							}
							// 	attribute Order.quantity for field qty			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("qty")) {
								if(nestedRow.getAs("qty")==null)
									places2.getOrder().setQuantity(null);
								else{
									places2.getOrder().setQuantity((Integer)nestedRow.getAs("qty"));
									toAdd2 = true;					
									}
							}
							if(toAdd2) {
								if((buyer_condition == null || buyer_refilter.booleanValue() || buyer_condition.evaluate(places2.getBuyer()))&&(order_condition == null || order_refilter.booleanValue() || order_condition.evaluate(places2.getOrder())))
								list_res.add(places2);
								addedInList = true;
							} 
							if(addedInList)
								toAdd1 = false;
						}
					}
					
					if(toAdd1) {
						
							list_res.add(places1);
						addedInList = true;
					} 
					
					
					
					return list_res.iterator();
		
			}, Encoders.bean(Places.class));
			// TODO drop duplicates based on roles ids
			//res= res.dropDuplicates(new String {});
			return res;
	}
	
	
	
	
	public java.util.List<pojo.Places> getPlacesList(
		conditions.Condition<conditions.CustomerAttribute> buyer_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition){
			//TODO
			return null;
		}
	
	public java.util.List<pojo.Places> getPlacesListByBuyerCondition(
		conditions.Condition<conditions.CustomerAttribute> buyer_condition
	){
		return getPlacesList(buyer_condition, null);
	}
	
	public java.util.List<pojo.Places> getPlacesListByBuyer(pojo.Customer buyer) {
		// TODO using id for selecting
		return null;
	}
	public java.util.List<pojo.Places> getPlacesListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		return getPlacesList(null, order_condition);
	}
	
	public pojo.Places getPlacesByOrder(pojo.Order order) {
		// TODO using id for selecting
		return null;
	}
	
	public void insertPlacesAndLinkedItems(pojo.Places places){
		//TODO
	}
	
	
	
	public void deletePlacesList(
		conditions.Condition<conditions.CustomerAttribute> buyer_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition){
			//TODO
		}
	
	public void deletePlacesListByBuyerCondition(
		conditions.Condition<conditions.CustomerAttribute> buyer_condition
	){
		deletePlacesList(buyer_condition, null);
	}
	
	public void deletePlacesListByBuyer(pojo.Customer buyer) {
		// TODO using id for selecting
		return;
	}
	public void deletePlacesListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deletePlacesList(null, order_condition);
	}
	
	public void deletePlacesByOrder(pojo.Order order) {
		// TODO using id for selecting
		return;
	}
		
}
