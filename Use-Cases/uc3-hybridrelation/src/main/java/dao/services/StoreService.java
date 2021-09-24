package dao.services;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pojo.Store;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.commons.lang.mutable.MutableBoolean;
import conditions.Condition;
import conditions.Operator;
import util.Util;
import conditions.StoreAttribute;
import conditions.OrderAttribute;
import pojo.Order;

public abstract class StoreService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StoreService.class);
	protected FromService fromService = new dao.impl.FromServiceImpl();
	


	public static enum ROLE_NAME {
		FROM_STORE
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.FROM_STORE, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public StoreService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public StoreService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
		this();
		if(loadingParams != null)
			for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: loadingParams.entrySet())
				loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public static java.util.Map<ROLE_NAME, loading.Loading> getDefaultLoadingParameters() {
		java.util.Map<ROLE_NAME, loading.Loading> res = new java.util.HashMap<ROLE_NAME, loading.Loading>();
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
				res.put(entry.getKey(), entry.getValue());
		return res;
	}
	
	public static void setAllDefaultLoadingParameters(loading.Loading loading) {
		java.util.Map<ROLE_NAME, loading.Loading> newParams = new java.util.HashMap<ROLE_NAME, loading.Loading>();
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
				newParams.put(entry.getKey(), entry.getValue());
		defaultLoadingParameters = newParams;
	}
	
	public java.util.Map<ROLE_NAME, loading.Loading> getLoadingParameters() {
		return this.loadingParameters;
	}
	
	public void setLoadingParameters(java.util.Map<ROLE_NAME, loading.Loading> newParams) {
		this.loadingParameters = newParams;
	}
	
	public void updateLoadingParameter(ROLE_NAME role, loading.Loading l) {
		this.loadingParameters.put(role, l);
	}
	
	
	public Dataset<Store> getStoreList(conditions.Condition<conditions.StoreAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Store>> datasets = new ArrayList<Dataset<Store>>();
		Dataset<Store> d = null;
		d = getStoreListInSTOREFromINVENTORY(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsStore(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Store>) r -> condition == null || condition.evaluate(r));
		d=d.dropDuplicates();
		return d;
	}
	
	
	
	
	public abstract Dataset<Store> getStoreListInSTOREFromINVENTORY(conditions.Condition<conditions.StoreAttribute> condition, MutableBoolean refilterFlag);
	
	
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
	
	
	
	protected static Dataset<Store> fullOuterJoinsStore(List<Dataset<Store>> datasetsPOJO) {
		return fullOuterJoinsStore(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Store> fullLeftOuterJoinsStore(List<Dataset<Store>> datasetsPOJO) {
		return fullOuterJoinsStore(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Store> fullOuterJoinsStore(List<Dataset<Store>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Store> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("vAT", "vAT_1")
								.withColumnRenamed("address", "address_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("vAT", "vAT_" + i)
								.withColumnRenamed("address", "address_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			} 
			d = res.map((MapFunction<Row, Store>) r -> {
					Store store_res = new Store();
					
					// attribute 'Store.id'
					Integer firstNotNull_id = Util.getIntegerValue(r.getAs("id"));
					store_res.setId(firstNotNull_id);
					
					// attribute 'Store.vAT'
					String firstNotNull_VAT = Util.getStringValue(r.getAs("vAT"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String vAT2 = Util.getStringValue(r.getAs("vAT_" + i));
						if (firstNotNull_VAT != null && vAT2 != null && !firstNotNull_VAT.equals(vAT2)) {
							store_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Store.vAT': " + firstNotNull_VAT + " and " + vAT2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Store.vAT' ==> " + firstNotNull_VAT + " and " + vAT2);
						}
						if (firstNotNull_VAT == null && vAT2 != null) {
							firstNotNull_VAT = vAT2;
						}
					}
					store_res.setVAT(firstNotNull_VAT);
					
					// attribute 'Store.address'
					String firstNotNull_address = Util.getStringValue(r.getAs("address"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String address2 = Util.getStringValue(r.getAs("address_" + i));
						if (firstNotNull_address != null && address2 != null && !firstNotNull_address.equals(address2)) {
							store_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Store.address': " + firstNotNull_address + " and " + address2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Store.address' ==> " + firstNotNull_address + " and " + address2);
						}
						if (firstNotNull_address == null && address2 != null) {
							firstNotNull_address = address2;
						}
					}
					store_res.setAddress(firstNotNull_address);
	
					scala.collection.mutable.WrappedArray<String> logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							store_res.addLogEvent(logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							store_res.addLogEvent(logEvents.apply(j));
						}
					}
	
					return store_res;
				}, Encoders.bean(Store.class));
			return d;
	}
	
	
	
	
	public Store getStore(Store.from role, Order order) {
		if(role != null) {
			if(role.equals(Store.from.store))
				return getStoreInFromByOrder(order);
		}
		return null;
	}
	
	public Dataset<Store> getStoreList(Store.from role, Condition<OrderAttribute> condition) {
		if(role != null) {
			if(role.equals(Store.from.store))
				return getStoreListInFromByOrderCondition(condition);
		}
		return null;
	}
	
	public Dataset<Store> getStoreList(Store.from role, Condition<StoreAttribute> condition1, Condition<OrderAttribute> condition2) {
		if(role != null) {
			if(role.equals(Store.from.store))
				return getStoreListInFrom(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	public abstract Dataset<Store> getStoreListInFrom(conditions.Condition<conditions.StoreAttribute> store_condition,conditions.Condition<conditions.OrderAttribute> order_condition);
	
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
	
	
	public abstract boolean insertStore(Store store);
	
	public abstract boolean insertStoreInSTOREFromINVENTORY(Store store); 
	
	public abstract void updateStoreList(conditions.Condition<conditions.StoreAttribute> condition, conditions.SetClause<conditions.StoreAttribute> set);
	
	public void updateStore(pojo.Store store) {
		//TODO using the id
		return;
	}
	public abstract void updateStoreListInFrom(
		conditions.Condition<conditions.StoreAttribute> store_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition,
		
		conditions.SetClause<conditions.StoreAttribute> set
	);
	
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
	
	
	
	public abstract void deleteStoreList(conditions.Condition<conditions.StoreAttribute> condition);
	
	public void deleteStore(pojo.Store store) {
		//TODO using the id
		return;
	}
	public abstract void deleteStoreListInFrom(	
		conditions.Condition<conditions.StoreAttribute> store_condition,	
		conditions.Condition<conditions.OrderAttribute> order_condition);
	
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
