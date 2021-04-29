package dao.services;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pojo.Order;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.commons.lang.mutable.MutableBoolean;
import conditions.Condition;
import conditions.Operator;
import conditions.OrderAttribute;
import conditions.CustomerAttribute;
import pojo.Customer;
import conditions.OrderAttribute;
import conditions.ProductAttribute;
import pojo.Product;
import conditions.OrderAttribute;
import conditions.StoreAttribute;
import pojo.Store;

public abstract class OrderService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OrderService.class);
	protected PlacesService placesService = new dao.impl.PlacesServiceImpl();
	protected OfService ofService = new dao.impl.OfServiceImpl();
	protected FromService fromService = new dao.impl.FromServiceImpl();
	


	public static enum ROLE_NAME {
		PLACES_ORDER, OF_ORDER, FROM_ORDER
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.PLACES_ORDER, loading.Loading.EAGER);
		defaultLoadingParameters.put(ROLE_NAME.OF_ORDER, loading.Loading.EAGER);
		defaultLoadingParameters.put(ROLE_NAME.FROM_ORDER, loading.Loading.EAGER);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public OrderService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public OrderService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<Order> getOrderList(conditions.Condition<conditions.OrderAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Order>> datasets = new ArrayList<Dataset<Order>>();
		Dataset<Order> d = null;
		d = getOrderListInClientCollectionFromMongo(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
	
		
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasets.get(1)
								.withColumnRenamed("quantity", "quantity_1")
							, seq, "fullouter");
			for(int i = 2; i < datasets.size(); i++) {
				res = res.join(datasets.get(i)
								.withColumnRenamed("quantity", "quantity_" + i)
							, seq, "fullouter");
			} 
			d = res.map((MapFunction<Row, Order>) r -> {
					Order order_res = new Order();
					
					// attribute 'Order.id'
					Integer firstNotNull_id = r.getAs("id");
					order_res.setId(firstNotNull_id);
					
					// attribute 'Order.quantity'
					Integer firstNotNull_quantity = r.getAs("quantity");
					for (int i = 1; i < datasets.size(); i++) {
						Integer quantity2 = r.getAs("quantity_" + i);
						if (firstNotNull_quantity != null && quantity2 != null && !firstNotNull_quantity.equals(quantity2)) {
							order_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Order.quantity': " + firstNotNull_quantity + " and " + quantity2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Order.quantity' ==> " + firstNotNull_quantity + " and " + quantity2);
						}
						if (firstNotNull_quantity == null && quantity2 != null) {
							firstNotNull_quantity = quantity2;
						}
					}
					order_res.setQuantity(firstNotNull_quantity);
					return order_res;
				}, Encoders.bean(Order.class));
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Order>) r -> condition == null || condition.evaluate(r));
		d=d.dropDuplicates();
		return d;
	}
	
	
	
	public abstract Dataset<Order> getOrderListInClientCollectionFromMongo(conditions.Condition<conditions.OrderAttribute> condition, MutableBoolean refilterFlag);
	
	
	// TODO get based on id(s). Ex:public Client getClientById(Long id)
	
	public Dataset<Order> getOrderListById(Integer id) {
		return getOrderList(conditions.Condition.simple(conditions.OrderAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Order> getOrderListByQuantity(Integer quantity) {
		return getOrderList(conditions.Condition.simple(conditions.OrderAttribute.quantity, conditions.Operator.EQUALS, quantity));
	}
	
	
	
	protected static Dataset<Order> fullOuterJoinsOrder(List<Dataset<Order>> datasetsPOJO) {
		return fullOuterJoinsOrder(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Order> fullLeftOuterJoinsOrder(List<Dataset<Order>> datasetsPOJO) {
		return fullOuterJoinsOrder(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Order> fullOuterJoinsOrder(List<Dataset<Order>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Order> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("quantity", "quantity_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("quantity", "quantity_" + i)
							, seq, joinMode);
			} 
			d = res.map((MapFunction<Row, Order>) r -> {
					Order order_res = new Order();
					
					// attribute 'Order.id'
					Integer firstNotNull_id = r.getAs("id");
					order_res.setId(firstNotNull_id);
					
					// attribute 'Order.quantity'
					Integer firstNotNull_quantity = r.getAs("quantity");
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer quantity2 = r.getAs("quantity_" + i);
						if (firstNotNull_quantity != null && quantity2 != null && !firstNotNull_quantity.equals(quantity2)) {
							order_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Order.quantity': " + firstNotNull_quantity + " and " + quantity2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Order.quantity' ==> " + firstNotNull_quantity + " and " + quantity2);
						}
						if (firstNotNull_quantity == null && quantity2 != null) {
							firstNotNull_quantity = quantity2;
						}
					}
					order_res.setQuantity(firstNotNull_quantity);
					return order_res;
				}, Encoders.bean(Order.class));
			return d;
	}
	
	
	public Dataset<Order> getOrderList(Order.places role, Customer customer) {
		if(role != null) {
			if(role.equals(Order.places.order))
				return getOrderListInPlacesByBuyer(customer);
		}
		return null;
	}
	
	public Dataset<Order> getOrderList(Order.places role, Condition<CustomerAttribute> condition) {
		if(role != null) {
			if(role.equals(Order.places.order))
				return getOrderListInPlacesByBuyerCondition(condition);
		}
		return null;
	}
	
	public Dataset<Order> getOrderList(Order.places role, Condition<CustomerAttribute> condition1, Condition<OrderAttribute> condition2) {
		if(role != null) {
			if(role.equals(Order.places.order))
				return getOrderListInPlaces(condition1, condition2);
		}
		return null;
	}
	
	
	
	public Dataset<Order> getOrderList(Order.of role, Product product) {
		if(role != null) {
			if(role.equals(Order.of.order))
				return getOrderListInOfByBought_item(product);
		}
		return null;
	}
	
	public Dataset<Order> getOrderList(Order.of role, Condition<ProductAttribute> condition) {
		if(role != null) {
			if(role.equals(Order.of.order))
				return getOrderListInOfByBought_itemCondition(condition);
		}
		return null;
	}
	
	public Dataset<Order> getOrderList(Order.of role, Condition<ProductAttribute> condition1, Condition<OrderAttribute> condition2) {
		if(role != null) {
			if(role.equals(Order.of.order))
				return getOrderListInOf(condition1, condition2);
		}
		return null;
	}
	
	
	
	public Dataset<Order> getOrderList(Order.from role, Store store) {
		if(role != null) {
			if(role.equals(Order.from.order))
				return getOrderListInFromByStore(store);
		}
		return null;
	}
	
	public Dataset<Order> getOrderList(Order.from role, Condition<StoreAttribute> condition) {
		if(role != null) {
			if(role.equals(Order.from.order))
				return getOrderListInFromByStoreCondition(condition);
		}
		return null;
	}
	
	public Dataset<Order> getOrderList(Order.from role, Condition<StoreAttribute> condition1, Condition<OrderAttribute> condition2) {
		if(role != null) {
			if(role.equals(Order.from.order))
				return getOrderListInFrom(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	public abstract Dataset<Order> getOrderListInPlaces(conditions.Condition<conditions.CustomerAttribute> buyer_condition,conditions.Condition<conditions.OrderAttribute> order_condition);
	
	public Dataset<Order> getOrderListInPlacesByBuyerCondition(conditions.Condition<conditions.CustomerAttribute> buyer_condition){
		return getOrderListInPlaces(buyer_condition, null);
	}
	
	public Dataset<Order> getOrderListInPlacesByBuyer(pojo.Customer buyer){
		if(buyer == null)
			return null;
	
		Condition c;
		c=Condition.simple(CustomerAttribute.id,Operator.EQUALS, buyer.getId());
		Dataset<Order> res = getOrderListInPlacesByBuyerCondition(c);
		return res;
	}
	
	public Dataset<Order> getOrderListInPlacesByOrderCondition(conditions.Condition<conditions.OrderAttribute> order_condition){
		return getOrderListInPlaces(null, order_condition);
	}
	public abstract Dataset<Order> getOrderListInOf(conditions.Condition<conditions.ProductAttribute> bought_item_condition,conditions.Condition<conditions.OrderAttribute> order_condition);
	
	public Dataset<Order> getOrderListInOfByBought_itemCondition(conditions.Condition<conditions.ProductAttribute> bought_item_condition){
		return getOrderListInOf(bought_item_condition, null);
	}
	
	public Dataset<Order> getOrderListInOfByBought_item(pojo.Product bought_item){
		if(bought_item == null)
			return null;
	
		Condition c;
		c=Condition.simple(ProductAttribute.id,Operator.EQUALS, bought_item.getId());
		Dataset<Order> res = getOrderListInOfByBought_itemCondition(c);
		return res;
	}
	
	public Dataset<Order> getOrderListInOfByOrderCondition(conditions.Condition<conditions.OrderAttribute> order_condition){
		return getOrderListInOf(null, order_condition);
	}
	public abstract Dataset<Order> getOrderListInFrom(conditions.Condition<conditions.StoreAttribute> store_condition,conditions.Condition<conditions.OrderAttribute> order_condition);
	
	public Dataset<Order> getOrderListInFromByStoreCondition(conditions.Condition<conditions.StoreAttribute> store_condition){
		return getOrderListInFrom(store_condition, null);
	}
	
	public Dataset<Order> getOrderListInFromByStore(pojo.Store store){
		if(store == null)
			return null;
	
		Condition c;
		c=Condition.simple(StoreAttribute.id,Operator.EQUALS, store.getId());
		Dataset<Order> res = getOrderListInFromByStoreCondition(c);
		return res;
	}
	
	public Dataset<Order> getOrderListInFromByOrderCondition(conditions.Condition<conditions.OrderAttribute> order_condition){
		return getOrderListInFrom(null, order_condition);
	}
	
	public abstract void insertOrderAndLinkedItems(Order order);
	public abstract void insertOrder(
		Order order,
		pojo.Customer persistentPlacesBuyer,
		pojo.Product persistentOfBought_item,
		pojo.Store persistentFromStore);
	
	public abstract void insertOrderInClientCollectionFromMongo(Order order); 
	public abstract void updateOrderList(conditions.Condition<conditions.OrderAttribute> condition, conditions.SetClause<conditions.OrderAttribute> set);
	
	public void updateOrder(pojo.Order order) {
		//TODO using the id
		return;
	}
	public abstract void updateOrderListInPlaces(
		conditions.Condition<conditions.CustomerAttribute> buyer_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition,
		
		conditions.SetClause<conditions.OrderAttribute> set
	);
	
	public void updateOrderListInPlacesByBuyerCondition(
		conditions.Condition<conditions.CustomerAttribute> buyer_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInPlaces(buyer_condition, null, set);
	}
	
	public void updateOrderListInPlacesByBuyer(
		pojo.Customer buyer,
		conditions.SetClause<conditions.OrderAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateOrderListInPlacesByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInPlaces(null, order_condition, set);
	}
	public abstract void updateOrderListInOf(
		conditions.Condition<conditions.ProductAttribute> bought_item_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition,
		
		conditions.SetClause<conditions.OrderAttribute> set
	);
	
	public void updateOrderListInOfByBought_itemCondition(
		conditions.Condition<conditions.ProductAttribute> bought_item_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInOf(bought_item_condition, null, set);
	}
	
	public void updateOrderListInOfByBought_item(
		pojo.Product bought_item,
		conditions.SetClause<conditions.OrderAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateOrderListInOfByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInOf(null, order_condition, set);
	}
	public abstract void updateOrderListInFrom(
		conditions.Condition<conditions.StoreAttribute> store_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition,
		
		conditions.SetClause<conditions.OrderAttribute> set
	);
	
	public void updateOrderListInFromByStoreCondition(
		conditions.Condition<conditions.StoreAttribute> store_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInFrom(store_condition, null, set);
	}
	
	public void updateOrderListInFromByStore(
		pojo.Store store,
		conditions.SetClause<conditions.OrderAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateOrderListInFromByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInFrom(null, order_condition, set);
	}
	
	
	public abstract void deleteOrderList(conditions.Condition<conditions.OrderAttribute> condition);
	
	public void deleteOrder(pojo.Order order) {
		//TODO using the id
		return;
	}
	public abstract void deleteOrderListInPlaces(	
		conditions.Condition<conditions.CustomerAttribute> buyer_condition,	
		conditions.Condition<conditions.OrderAttribute> order_condition);
	
	public void deleteOrderListInPlacesByBuyerCondition(
		conditions.Condition<conditions.CustomerAttribute> buyer_condition
	){
		deleteOrderListInPlaces(buyer_condition, null);
	}
	
	public void deleteOrderListInPlacesByBuyer(
		pojo.Customer buyer 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteOrderListInPlacesByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteOrderListInPlaces(null, order_condition);
	}
	public abstract void deleteOrderListInOf(	
		conditions.Condition<conditions.ProductAttribute> bought_item_condition,	
		conditions.Condition<conditions.OrderAttribute> order_condition);
	
	public void deleteOrderListInOfByBought_itemCondition(
		conditions.Condition<conditions.ProductAttribute> bought_item_condition
	){
		deleteOrderListInOf(bought_item_condition, null);
	}
	
	public void deleteOrderListInOfByBought_item(
		pojo.Product bought_item 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteOrderListInOfByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteOrderListInOf(null, order_condition);
	}
	public abstract void deleteOrderListInFrom(	
		conditions.Condition<conditions.StoreAttribute> store_condition,	
		conditions.Condition<conditions.OrderAttribute> order_condition);
	
	public void deleteOrderListInFromByStoreCondition(
		conditions.Condition<conditions.StoreAttribute> store_condition
	){
		deleteOrderListInFrom(store_condition, null);
	}
	
	public void deleteOrderListInFromByStore(
		pojo.Store store 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteOrderListInFromByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteOrderListInFrom(null, order_condition);
	}
	
}
