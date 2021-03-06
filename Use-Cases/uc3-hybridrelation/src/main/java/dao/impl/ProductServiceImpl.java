package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Product;
import conditions.*;
import dao.services.ProductService;
import util.Util;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.ArrayType;
import scala.Tuple2;
import org.bson.Document;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;


public class ProductServiceImpl extends ProductService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProductServiceImpl.class);
	
	
	
	public static Pair<String, List<String>> getSQLWhereClauseInPRODUCTFromINVENTORY(Condition<ProductAttribute> condition, MutableBoolean refilterFlag) {
		return getSQLWhereClauseInPRODUCTFromINVENTORYWithTableAlias(condition, refilterFlag, "");
	}
	
	public static Pair<String, List<String>> getSQLWhereClauseInPRODUCTFromINVENTORYWithTableAlias(Condition<ProductAttribute> condition, MutableBoolean refilterFlag, String tableAlias) {
		String where = null;	
		List<String> preparedValues = new java.util.ArrayList<String>();
		if(condition != null) {
			
			if(condition instanceof SimpleCondition) {
				ProductAttribute attr = ((SimpleCondition<ProductAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<ProductAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<ProductAttribute>) condition).getValue();
				if(value != null) {
					boolean isConditionAttrEncountered = false;
					if(attr == ProductAttribute.id ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "ID " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(attr == ProductAttribute.label ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "NAME " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(attr == ProductAttribute.price ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "PRICE " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						where = "1 = 1";
					}
				}
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInPRODUCTFromINVENTORY(((AndCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInPRODUCTFromINVENTORY(((AndCondition) condition).getRightCondition(), refilterFlag);
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
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInPRODUCTFromINVENTORY(((OrCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInPRODUCTFromINVENTORY(((OrCondition) condition).getRightCondition(), refilterFlag);
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
	
	
	public Dataset<Product> getProductListInPRODUCTFromINVENTORY(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = ProductServiceImpl.getSQLWhereClauseInPRODUCTFromINVENTORY(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", "'" + Util.escapeQuote(preparedValue) + "'");
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("INVENTORY", "PRODUCT");
		if(where != null) {
			d = d.where(where);
		}
	
		Dataset<Product> res = d.map((MapFunction<Row, Product>) r -> {
					Product product_res = new Product();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Product.Id]
					Integer id = Util.getIntegerValue(r.getAs("ID"));
					product_res.setId(id);
					
					// attribute [Product.Label]
					String label = Util.getStringValue(r.getAs("NAME"));
					product_res.setLabel(label);
					
					// attribute [Product.Price]
					Double price = Util.getDoubleValue(r.getAs("PRICE"));
					product_res.setPrice(price);
	
	
	
					return product_res;
				}, Encoders.bean(Product.class));
	
	
		return res;
		
	}
	
	
	// TODO get based on id(s). Ex:public Client getClientById(Long id)
	
	public Dataset<Product> getProductListById(Integer id) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Product> getProductListByLabel(String label) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.label, conditions.Operator.EQUALS, label));
	}
	
	public Dataset<Product> getProductListByPrice(Double price) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.price, conditions.Operator.EQUALS, price));
	}
	
	
	
	
	public Dataset<Product> getBought_itemListInOf(conditions.Condition<conditions.ProductAttribute> bought_item_condition,conditions.Condition<conditions.OrderAttribute> order_condition)		{
		MutableBoolean bought_item_refilter = new MutableBoolean(false);
		List<Dataset<Product>> datasetsPOJO = new ArrayList<Dataset<Product>>();
		Dataset<Order> all = new OrderServiceImpl().getOrderList(order_condition);
		boolean all_already_persisted = false;
		MutableBoolean order_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		order_refilter = new MutableBoolean(false);
		// For role 'order' in reference 'buys' 
		order_refilter = new MutableBoolean(false);
		Dataset<OrderTDO> orderTDObuysorder = ofService.getOrderTDOListOrderInBuysInClientCollectionFromMongoSchema(order_condition, order_refilter);
		Dataset<ProductTDO> productTDObuysbought_item = ofService.getProductTDOListBought_itemInBuysInClientCollectionFromMongoSchema(bought_item_condition, bought_item_refilter);
		if(order_refilter.booleanValue()) {
			joinCondition = null;
			joinCondition = orderTDObuysorder.col("id").equalTo(all.col("id"));
			if(joinCondition == null)
				orderTDObuysorder = orderTDObuysorder.as("A").join(all).select("A.*").as(Encoders.bean(OrderTDO.class));
			else
				orderTDObuysorder = orderTDObuysorder.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(OrderTDO.class));
		}
		Dataset<Row> res_buys = productTDObuysbought_item.join(orderTDObuysorder
				.withColumnRenamed("id", "Order_id")
				.withColumnRenamed("quantity", "Order_quantity")
				.withColumnRenamed("logEvents", "Order_logEvents"),
				productTDObuysbought_item.col("mongoSchema_ClientCollection_buys_ID").equalTo(orderTDObuysorder.col("mongoSchema_ClientCollection_buys_productId")));
		Dataset<Product> res_Product_buys = res_buys.select( "id", "label", "price", "logEvents").as(Encoders.bean(Product.class));
		res_Product_buys = res_Product_buys.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Product_buys);
		
		Dataset<Of> res_of_bought_item;
		Dataset<Product> res_Product;
		
		
		//Join datasets or return 
		Dataset<Product> res = fullOuterJoinsProduct(datasetsPOJO);
		if(res == null)
			return null;
	
		if(bought_item_refilter.booleanValue())
			res = res.filter((FilterFunction<Product>) r -> bought_item_condition == null || bought_item_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Product> getBought_itemListInOfByBought_itemCondition(conditions.Condition<conditions.ProductAttribute> bought_item_condition){
		return getBought_itemListInOf(bought_item_condition, null);
	}
	public Dataset<Product> getBought_itemListInOfByOrderCondition(conditions.Condition<conditions.OrderAttribute> order_condition){
		return getBought_itemListInOf(null, order_condition);
	}
	
	public Product getBought_itemInOfByOrder(pojo.Order order){
		if(order == null)
			return null;
	
		Condition c;
		c=Condition.simple(OrderAttribute.id,Operator.EQUALS, order.getId());
		Dataset<Product> res = getBought_itemListInOfByOrderCondition(c);
		return !res.isEmpty()?res.first():null;
	}
	
	
	public boolean insertProduct(Product product){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertProductInPRODUCTFromINVENTORY(product) || inserted ;
		return inserted;
	}
	
	public boolean insertProductInPRODUCTFromINVENTORY(Product product)	{
		Condition<ProductAttribute> conditionID;
		String idvalue="";
		conditionID = Condition.simple(ProductAttribute.id, Operator.EQUALS, product.getId());
		idvalue+=product.getId();
		boolean entityExists=false;
		entityExists = !getProductListInPRODUCTFromINVENTORY(conditionID,new MutableBoolean(false)).isEmpty();
				
		if(!entityExists){
		List<Row> listRows=new ArrayList<Row>();
		List<Object> valuesPRODUCT_1 = new ArrayList<>();
		List<StructField> listOfStructFieldPRODUCT_1 = new ArrayList<StructField>();
		if(!listOfStructFieldPRODUCT_1.contains(DataTypes.createStructField("ID",DataTypes.IntegerType, true)))
			listOfStructFieldPRODUCT_1.add(DataTypes.createStructField("ID",DataTypes.IntegerType, true));
		valuesPRODUCT_1.add(product.getId());
		if(!listOfStructFieldPRODUCT_1.contains(DataTypes.createStructField("NAME",DataTypes.StringType, true)))
			listOfStructFieldPRODUCT_1.add(DataTypes.createStructField("NAME",DataTypes.StringType, true));
		valuesPRODUCT_1.add(product.getLabel());
		if(!listOfStructFieldPRODUCT_1.contains(DataTypes.createStructField("PRICE",DataTypes.FloatType, true)))
			listOfStructFieldPRODUCT_1.add(DataTypes.createStructField("PRICE",DataTypes.FloatType, true));
		valuesPRODUCT_1.add(product.getPrice());
		
		StructType structType = DataTypes.createStructType(listOfStructFieldPRODUCT_1);
		listRows.add(RowFactory.create(valuesPRODUCT_1.toArray()));
		SparkConnectionMgr.writeDataset(listRows, structType, "jdbc", "PRODUCT", "INVENTORY");
			logger.info("Inserted [Product] entity ID [{}] in [PRODUCT] in database [INVENTORY]", idvalue);
		}
		else
			logger.warn("[Product] entity ID [{}] already present in [PRODUCT] in database [INVENTORY]", idvalue);
		return !entityExists;
	} 
	
	
	public void updateProductList(conditions.Condition<conditions.ProductAttribute> condition, conditions.SetClause<conditions.ProductAttribute> set){
		//TODO
	}
	
	public void updateProduct(pojo.Product product) {
		//TODO using the id
		return;
	}
	public void updateBought_itemListInOf(
		conditions.Condition<conditions.ProductAttribute> bought_item_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition,
		
		conditions.SetClause<conditions.ProductAttribute> set
	){
		//TODO
	}
	
	public void updateBought_itemListInOfByBought_itemCondition(
		conditions.Condition<conditions.ProductAttribute> bought_item_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateBought_itemListInOf(bought_item_condition, null, set);
	}
	public void updateBought_itemListInOfByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateBought_itemListInOf(null, order_condition, set);
	}
	
	public void updateBought_itemInOfByOrder(
		pojo.Order order,
		conditions.SetClause<conditions.ProductAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	
	
	public void deleteProductList(conditions.Condition<conditions.ProductAttribute> condition){
		//TODO
	}
	
	public void deleteProduct(pojo.Product product) {
		//TODO using the id
		return;
	}
	public void deleteBought_itemListInOf(	
		conditions.Condition<conditions.ProductAttribute> bought_item_condition,	
		conditions.Condition<conditions.OrderAttribute> order_condition){
			//TODO
		}
	
	public void deleteBought_itemListInOfByBought_itemCondition(
		conditions.Condition<conditions.ProductAttribute> bought_item_condition
	){
		deleteBought_itemListInOf(bought_item_condition, null);
	}
	public void deleteBought_itemListInOfByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteBought_itemListInOf(null, order_condition);
	}
	
	public void deleteBought_itemInOfByOrder(
		pojo.Order order 
	){
		//TODO get id in condition
		return;	
	}
	
	
}
