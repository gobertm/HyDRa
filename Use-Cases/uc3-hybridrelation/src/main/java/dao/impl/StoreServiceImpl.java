package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Store;
import conditions.*;
import dao.services.StoreService;
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


public class StoreServiceImpl extends StoreService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StoreServiceImpl.class);
	
	
	
	public static Pair<String, List<String>> getSQLWhereClauseInSTOREFromINVENTORY(Condition<StoreAttribute> condition, MutableBoolean refilterFlag) {
		return getSQLWhereClauseInSTOREFromINVENTORYWithTableAlias(condition, refilterFlag, "");
	}
	
	public static Pair<String, List<String>> getSQLWhereClauseInSTOREFromINVENTORYWithTableAlias(Condition<StoreAttribute> condition, MutableBoolean refilterFlag, String tableAlias) {
		String where = null;	
		List<String> preparedValues = new java.util.ArrayList<String>();
		if(condition != null) {
			
			if(condition instanceof SimpleCondition) {
				StoreAttribute attr = ((SimpleCondition<StoreAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<StoreAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<StoreAttribute>) condition).getValue();
				if(value != null) {
					boolean isConditionAttrEncountered = false;
					if(attr == StoreAttribute.id ) {
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
					if(attr == StoreAttribute.VAT ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "VAT " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(attr == StoreAttribute.address ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "ADDR " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						where = "1 = 1";
					}
				}
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInSTOREFromINVENTORY(((AndCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInSTOREFromINVENTORY(((AndCondition) condition).getRightCondition(), refilterFlag);
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
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInSTOREFromINVENTORY(((OrCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInSTOREFromINVENTORY(((OrCondition) condition).getRightCondition(), refilterFlag);
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
	
	
	public Dataset<Store> getStoreListInSTOREFromINVENTORY(conditions.Condition<conditions.StoreAttribute> condition, MutableBoolean refilterFlag){
	
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
	
		Dataset<Store> res = d.map((MapFunction<Row, Store>) r -> {
					Store store_res = new Store();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Store.Id]
					Integer id = Util.getIntegerValue(r.getAs("ID"));
					store_res.setId(id);
					
					// attribute [Store.VAT]
					String vAT = Util.getStringValue(r.getAs("VAT"));
					store_res.setVAT(vAT);
					
					// attribute [Store.Address]
					String address = Util.getStringValue(r.getAs("ADDR"));
					store_res.setAddress(address);
	
	
	
					return store_res;
				}, Encoders.bean(Store.class));
	
	
		return res;
		
	}
	
	
	// TODO get based on id(s). Ex:public Client getClientById(Long id)
	
	public Dataset<Store> getStoreListById(Integer id) {
		return getStoreList(conditions.Condition.simple(conditions.StoreAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Store> getStoreListByVAT(String VAT) {
		return getStoreList(conditions.Condition.simple(conditions.StoreAttribute.VAT, conditions.Operator.EQUALS, VAT));
	}
	
	public Dataset<Store> getStoreListByAddress(String address) {
		return getStoreList(conditions.Condition.simple(conditions.StoreAttribute.address, conditions.Operator.EQUALS, address));
	}
	
	
	
	
	public Dataset<Store> getStoreListInFrom(conditions.Condition<conditions.StoreAttribute> store_condition,conditions.Condition<conditions.OrderAttribute> order_condition)		{
		MutableBoolean store_refilter = new MutableBoolean(false);
		List<Dataset<Store>> datasetsPOJO = new ArrayList<Dataset<Store>>();
		Dataset<Order> all = new OrderServiceImpl().getOrderList(order_condition);
		boolean all_already_persisted = false;
		MutableBoolean order_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		order_refilter = new MutableBoolean(false);
		// For role 'order' in reference 'buys_in' 
		order_refilter = new MutableBoolean(false);
		Dataset<OrderTDO> orderTDObuys_inorder = fromService.getOrderTDOListOrderInBuys_inInClientCollectionFromMongoSchema(order_condition, order_refilter);
		Dataset<StoreTDO> storeTDObuys_instore = fromService.getStoreTDOListStoreInBuys_inInClientCollectionFromMongoSchema(store_condition, store_refilter);
		if(order_refilter.booleanValue()) {
			joinCondition = null;
			joinCondition = orderTDObuys_inorder.col("id").equalTo(all.col("id"));
			if(joinCondition == null)
				orderTDObuys_inorder = orderTDObuys_inorder.as("A").join(all).select("A.*").as(Encoders.bean(OrderTDO.class));
			else
				orderTDObuys_inorder = orderTDObuys_inorder.as("A").join(all, joinCondition).select("A.*").as(Encoders.bean(OrderTDO.class));
		}
		Dataset<Row> res_buys_in = storeTDObuys_instore.join(orderTDObuys_inorder
				.withColumnRenamed("id", "Order_id")
				.withColumnRenamed("quantity", "Order_quantity")
				.withColumnRenamed("logEvents", "Order_logEvents"),
				storeTDObuys_instore.col("mongoSchema_ClientCollection_buys_in_ID").equalTo(orderTDObuys_inorder.col("mongoSchema_ClientCollection_buys_in_storeId")));
		Dataset<Store> res_Store_buys_in = res_buys_in.select( "id", "VAT", "address", "logEvents").as(Encoders.bean(Store.class));
		res_Store_buys_in = res_Store_buys_in.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Store_buys_in);
		
		Dataset<From> res_from_store;
		Dataset<Store> res_Store;
		
		
		//Join datasets or return 
		Dataset<Store> res = fullOuterJoinsStore(datasetsPOJO);
		if(res == null)
			return null;
	
		if(store_refilter.booleanValue())
			res = res.filter((FilterFunction<Store>) r -> store_condition == null || store_condition.evaluate(r));
		
	
		return res;
		}
	public Dataset<Store> getStoreListInFromByStoreCondition(conditions.Condition<conditions.StoreAttribute> store_condition){
		return getStoreListInFrom(store_condition, null);
	}
	public Dataset<Store> getStoreListInFromByOrderCondition(conditions.Condition<conditions.OrderAttribute> order_condition){
		return getStoreListInFrom(null, order_condition);
	}
	
	public Store getStoreInFromByOrder(pojo.Order order){
		if(order == null)
			return null;
	
		Condition c;
		c=Condition.simple(OrderAttribute.id,Operator.EQUALS, order.getId());
		Dataset<Store> res = getStoreListInFromByOrderCondition(c);
		return !res.isEmpty()?res.first():null;
	}
	
	
	public boolean insertStore(Store store){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertStoreInSTOREFromINVENTORY(store) || inserted ;
		return inserted;
	}
	
	public boolean insertStoreInSTOREFromINVENTORY(Store store)	{
		Condition<StoreAttribute> conditionID;
		String idvalue="";
		conditionID = Condition.simple(StoreAttribute.id, Operator.EQUALS, store.getId());
		idvalue+=store.getId();
		boolean entityExists=false;
		entityExists = !getStoreListInSTOREFromINVENTORY(conditionID,new MutableBoolean(false)).isEmpty();
				
		if(!entityExists){
		List<Row> listRows=new ArrayList<Row>();
		List<Object> valuesSTORE_1 = new ArrayList<>();
		List<StructField> listOfStructFieldSTORE_1 = new ArrayList<StructField>();
		if(!listOfStructFieldSTORE_1.contains(DataTypes.createStructField("ID",DataTypes.IntegerType, true)))
			listOfStructFieldSTORE_1.add(DataTypes.createStructField("ID",DataTypes.IntegerType, true));
		valuesSTORE_1.add(store.getId());
		if(!listOfStructFieldSTORE_1.contains(DataTypes.createStructField("VAT",DataTypes.StringType, true)))
			listOfStructFieldSTORE_1.add(DataTypes.createStructField("VAT",DataTypes.StringType, true));
		valuesSTORE_1.add(store.getVAT());
		if(!listOfStructFieldSTORE_1.contains(DataTypes.createStructField("ADDR",DataTypes.StringType, true)))
			listOfStructFieldSTORE_1.add(DataTypes.createStructField("ADDR",DataTypes.StringType, true));
		valuesSTORE_1.add(store.getAddress());
		
		StructType structType = DataTypes.createStructType(listOfStructFieldSTORE_1);
		listRows.add(RowFactory.create(valuesSTORE_1.toArray()));
		SparkConnectionMgr.writeDataset(listRows, structType, "jdbc", "STORE", "INVENTORY");
			logger.info("Inserted [Store] entity ID [{}] in [STORE] in database [INVENTORY]", idvalue);
		}
		else
			logger.warn("[Store] entity ID [{}] already present in [STORE] in database [INVENTORY]", idvalue);
		return !entityExists;
	} 
	
	
	public void updateStoreList(conditions.Condition<conditions.StoreAttribute> condition, conditions.SetClause<conditions.StoreAttribute> set){
		//TODO
	}
	
	public void updateStore(pojo.Store store) {
		//TODO using the id
		return;
	}
	public void updateStoreListInFrom(
		conditions.Condition<conditions.StoreAttribute> store_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition,
		
		conditions.SetClause<conditions.StoreAttribute> set
	){
		//TODO
	}
	
	public void updateStoreListInFromByStoreCondition(
		conditions.Condition<conditions.StoreAttribute> store_condition,
		conditions.SetClause<conditions.StoreAttribute> set
	){
		updateStoreListInFrom(store_condition, null, set);
	}
	public void updateStoreListInFromByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.StoreAttribute> set
	){
		updateStoreListInFrom(null, order_condition, set);
	}
	
	public void updateStoreInFromByOrder(
		pojo.Order order,
		conditions.SetClause<conditions.StoreAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	
	
	public void deleteStoreList(conditions.Condition<conditions.StoreAttribute> condition){
		//TODO
	}
	
	public void deleteStore(pojo.Store store) {
		//TODO using the id
		return;
	}
	public void deleteStoreListInFrom(	
		conditions.Condition<conditions.StoreAttribute> store_condition,	
		conditions.Condition<conditions.OrderAttribute> order_condition){
			//TODO
		}
	
	public void deleteStoreListInFromByStoreCondition(
		conditions.Condition<conditions.StoreAttribute> store_condition
	){
		deleteStoreListInFrom(store_condition, null);
	}
	public void deleteStoreListInFromByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteStoreListInFrom(null, order_condition);
	}
	
	public void deleteStoreInFromByOrder(
		pojo.Order order 
	){
		//TODO get id in condition
		return;	
	}
	
	
}
