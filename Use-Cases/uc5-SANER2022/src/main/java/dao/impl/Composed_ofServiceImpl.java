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
import conditions.Composed_ofAttribute;
import conditions.Operator;
import pojo.Composed_of;
import tdo.OrderTDO;
import tdo.Composed_ofTDO;
import pojo.Order;
import conditions.OrderAttribute;
import tdo.ProductTDO;
import tdo.Composed_ofTDO;
import pojo.Product;
import conditions.ProductAttribute;
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

public class Composed_ofServiceImpl extends dao.services.Composed_ofService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Composed_ofServiceImpl.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	
	// method accessing the embedded object Orderline mapped to role orderP
	public Dataset<pojo.Composed_of> getComposed_ofListIndocSchemaordersColOrderline(Condition<OrderAttribute> orderP_condition, Condition<ProductAttribute> orderedProducts_condition, MutableBoolean orderP_refilter, MutableBoolean orderedProducts_refilter){	
			List<String> bsons = new ArrayList<String>();
			String bson = null;
			bson = OrderServiceImpl.getBSONMatchQueryInOrdersColFromMongobench(orderP_condition ,orderP_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
			bson = ProductServiceImpl.getBSONMatchQueryInOrdersColFromMongobench(orderedProducts_condition ,orderedProducts_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
		
			String bsonQuery = bsons.size() == 0 ? null : "{$match: { $and: [" + String.join(",", bsons) + "] }}";
		
			Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mongobench", "ordersCol", bsonQuery);
		
			Dataset<Composed_of> res = dataset.flatMap((FlatMapFunction<Row, Composed_of>) r -> {
					List<Composed_of> list_res = new ArrayList<Composed_of>();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					Row nestedRow = null;
		
					boolean addedInList = false;
					Row r1 = r;
					Composed_of composed_of1 = new Composed_of();
					composed_of1.setOrderP(new Order());
					composed_of1.setOrderedProducts(new Product());
					
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Order.id for field OrderId			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderId")) {
						if(nestedRow.getAs("OrderId")==null)
							composed_of1.getOrderP().setId(null);
						else{
							composed_of1.getOrderP().setId(Util.getStringValue(nestedRow.getAs("OrderId")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.orderdate for field OrderDate			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("OrderDate")) {
						if(nestedRow.getAs("OrderDate")==null)
							composed_of1.getOrderP().setOrderdate(null);
						else{
							composed_of1.getOrderP().setOrderdate(Util.getLocalDateValue(nestedRow.getAs("OrderDate")));
							toAdd1 = true;					
							}
					}
					// 	attribute Order.totalprice for field TotalPrice			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("TotalPrice")) {
						if(nestedRow.getAs("TotalPrice")==null)
							composed_of1.getOrderP().setTotalprice(null);
						else{
							composed_of1.getOrderP().setTotalprice(Util.getDoubleValue(nestedRow.getAs("TotalPrice")));
							toAdd1 = true;					
							}
					}
					array1 = r1.getAs("Orderline");
					if(array1!= null) {
						for (int i2 = 0; i2 < array1.size(); i2++){
							Row r2 = (Row) array1.apply(i2);
							Composed_of composed_of2 = (Composed_of) composed_of1.clone();
							boolean toAdd2  = false;
							WrappedArray array2  = null;
							// 	attribute Product.id for field asin			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("asin")) {
								if(nestedRow.getAs("asin")==null)
									composed_of2.getOrderedProducts().setId(null);
								else{
									composed_of2.getOrderedProducts().setId(Util.getStringValue(nestedRow.getAs("asin")));
									toAdd2 = true;					
									}
							}
							// 	attribute Product.title for field title			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("title")) {
								if(nestedRow.getAs("title")==null)
									composed_of2.getOrderedProducts().setTitle(null);
								else{
									composed_of2.getOrderedProducts().setTitle(Util.getStringValue(nestedRow.getAs("title")));
									toAdd2 = true;					
									}
							}
							// 	attribute Product.price for field price			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("price")) {
								if(nestedRow.getAs("price")==null)
									composed_of2.getOrderedProducts().setPrice(null);
								else{
									composed_of2.getOrderedProducts().setPrice(Util.getDoubleValue(nestedRow.getAs("price")));
									toAdd2 = true;					
									}
							}
							if(toAdd2 && ((orderP_condition == null || orderP_refilter.booleanValue() || orderP_condition.evaluate(composed_of2.getOrderP()))&&(orderedProducts_condition == null || orderedProducts_refilter.booleanValue() || orderedProducts_condition.evaluate(composed_of2.getOrderedProducts())))) {
								list_res.add(composed_of2);
								addedInList = true;
							} 
							if(addedInList)
								toAdd1 = false;
						}
					}
					
					if(toAdd1 ) {
						list_res.add(composed_of1);
						addedInList = true;
					} 
					
					
					
					return list_res.iterator();
		
			}, Encoders.bean(Composed_of.class));
			// TODO drop duplicates based on roles ids
			//res= res.dropDuplicates(new String {});
			return res;
	}
	
	
	
	
	public java.util.List<pojo.Composed_of> getComposed_ofList(
		conditions.Condition<conditions.OrderAttribute> orderP_condition,
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition){
			//TODO
			return null;
		}
	
	public java.util.List<pojo.Composed_of> getComposed_ofListByOrderPCondition(
		conditions.Condition<conditions.OrderAttribute> orderP_condition
	){
		return getComposed_ofList(orderP_condition, null);
	}
	
	public java.util.List<pojo.Composed_of> getComposed_ofListByOrderP(pojo.Order orderP) {
		// TODO using id for selecting
		return null;
	}
	public java.util.List<pojo.Composed_of> getComposed_ofListByOrderedProductsCondition(
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition
	){
		return getComposed_ofList(null, orderedProducts_condition);
	}
	
	public java.util.List<pojo.Composed_of> getComposed_ofListByOrderedProducts(pojo.Product orderedProducts) {
		// TODO using id for selecting
		return null;
	}
	
	public void insertComposed_of(Composed_of composed_of){
		//Link entities in join structures.
		// Update embedded structures mapped to non mandatory roles.
		insertComposed_ofInEmbeddedStructOrdersColInMongobench(composed_of);
		// Update ref fields mapped to non mandatory roles. 
	}
	
	
	public 	boolean insertComposed_ofInEmbeddedStructOrdersColInMongobench(Composed_of composed_of){
	 	// Rel 'composed_of' Insert in embedded structure 'ordersCol'
	
		Order order = composed_of.getOrderP();
		Product product = composed_of.getOrderedProducts();
		Bson filter= new Document();
		Bson updateOp;
		String addToSet;
		List<String> fieldName= new ArrayList();
		List<Bson> arrayFilterCond = new ArrayList();
		Document docOrderline_1 = new Document();
		docOrderline_1.append("asin",product.getId());
		docOrderline_1.append("title",product.getTitle());
		docOrderline_1.append("price",product.getPrice());
		
		// level 1 ascending
		filter = eq("OrderId",order.getId());
		updateOp = addToSet("Orderline", docOrderline_1);
		DBConnectionMgr.update(filter, updateOp, "ordersCol", "mongobench");					
		return true;
	}
	
	
	
	
	
	public void deleteComposed_ofList(
		conditions.Condition<conditions.OrderAttribute> orderP_condition,
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition){
			//TODO
		}
	
	public void deleteComposed_ofListByOrderPCondition(
		conditions.Condition<conditions.OrderAttribute> orderP_condition
	){
		deleteComposed_ofList(orderP_condition, null);
	}
	
	public void deleteComposed_ofListByOrderP(pojo.Order orderP) {
		// TODO using id for selecting
		return;
	}
	public void deleteComposed_ofListByOrderedProductsCondition(
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition
	){
		deleteComposed_ofList(null, orderedProducts_condition);
	}
	
	public void deleteComposed_ofListByOrderedProducts(pojo.Product orderedProducts) {
		// TODO using id for selecting
		return;
	}
		
}
