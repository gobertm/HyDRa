package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Order;
import java.time.LocalDate;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.commons.lang.mutable.MutableBoolean;
import conditions.Condition;
import conditions.Operator;
import util.Util;
import conditions.OrderAttribute;
import conditions.CustomerAttribute;
import pojo.Customer;
import conditions.OrderAttribute;
import conditions.ProductAttribute;
import pojo.Product;

public abstract class OrderService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OrderService.class);
	protected BuysService buysService = new dao.impl.BuysServiceImpl();
	protected Composed_ofService composed_ofService = new dao.impl.Composed_ofServiceImpl();
	


	public static enum ROLE_NAME {
		BUYS_ORDER, COMPOSED_OF_ORDERP
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.BUYS_ORDER, loading.Loading.EAGER);
		defaultLoadingParameters.put(ROLE_NAME.COMPOSED_OF_ORDERP, loading.Loading.LAZY);
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
	
	
	public Dataset<Order> getOrderList(){
		return getOrderList(null);
	}
	
	public Dataset<Order> getOrderList(conditions.Condition<conditions.OrderAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Order>> datasets = new ArrayList<Dataset<Order>>();
		Dataset<Order> d = null;
		d = getOrderListInOrdersColFromMongobench(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsOrder(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Order>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"id"});
		return d;
	}
	
	
	
	
	public abstract Dataset<Order> getOrderListInOrdersColFromMongobench(conditions.Condition<conditions.OrderAttribute> condition, MutableBoolean refilterFlag);
	
	
	public Order getOrderById(String id){
		Condition cond;
		cond = Condition.simple(OrderAttribute.id, conditions.Operator.EQUALS, id);
		Dataset<Order> res = getOrderList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<Order> getOrderListById(String id) {
		return getOrderList(conditions.Condition.simple(conditions.OrderAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Order> getOrderListByOrderdate(LocalDate orderdate) {
		return getOrderList(conditions.Condition.simple(conditions.OrderAttribute.orderdate, conditions.Operator.EQUALS, orderdate));
	}
	
	public Dataset<Order> getOrderListByTotalprice(Double totalprice) {
		return getOrderList(conditions.Condition.simple(conditions.OrderAttribute.totalprice, conditions.Operator.EQUALS, totalprice));
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
								.withColumnRenamed("orderdate", "orderdate_1")
								.withColumnRenamed("totalprice", "totalprice_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("orderdate", "orderdate_" + i)
								.withColumnRenamed("totalprice", "totalprice_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			} 
			d = res.map((MapFunction<Row, Order>) r -> {
					Order order_res = new Order();
					
					// attribute 'Order.id'
					String firstNotNull_id = Util.getStringValue(r.getAs("id"));
					order_res.setId(firstNotNull_id);
					
					// attribute 'Order.orderdate'
					LocalDate firstNotNull_orderdate = Util.getLocalDateValue(r.getAs("orderdate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate orderdate2 = Util.getLocalDateValue(r.getAs("orderdate_" + i));
						if (firstNotNull_orderdate != null && orderdate2 != null && !firstNotNull_orderdate.equals(orderdate2)) {
							order_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.orderdate': " + firstNotNull_orderdate + " and " + orderdate2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.orderdate': " + firstNotNull_orderdate + " and " + orderdate2 + "." );
						}
						if (firstNotNull_orderdate == null && orderdate2 != null) {
							firstNotNull_orderdate = orderdate2;
						}
					}
					order_res.setOrderdate(firstNotNull_orderdate);
					
					// attribute 'Order.totalprice'
					Double firstNotNull_totalprice = Util.getDoubleValue(r.getAs("totalprice"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double totalprice2 = Util.getDoubleValue(r.getAs("totalprice_" + i));
						if (firstNotNull_totalprice != null && totalprice2 != null && !firstNotNull_totalprice.equals(totalprice2)) {
							order_res.addLogEvent("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.totalprice': " + firstNotNull_totalprice + " and " + totalprice2 + "." );
							logger.warn("Data consistency problem for [Order - id :"+order_res.getId()+"]: different values found for attribute 'Order.totalprice': " + firstNotNull_totalprice + " and " + totalprice2 + "." );
						}
						if (firstNotNull_totalprice == null && totalprice2 != null) {
							firstNotNull_totalprice = totalprice2;
						}
					}
					order_res.setTotalprice(firstNotNull_totalprice);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							order_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							order_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return order_res;
				}, Encoders.bean(Order.class));
			return d;
	}
	
	
	public Dataset<Order> getOrderList(Order.buys role, Customer customer) {
		if(role != null) {
			if(role.equals(Order.buys.order))
				return getOrderListInBuysByClient(customer);
		}
		return null;
	}
	
	public Dataset<Order> getOrderList(Order.buys role, Condition<CustomerAttribute> condition) {
		if(role != null) {
			if(role.equals(Order.buys.order))
				return getOrderListInBuysByClientCondition(condition);
		}
		return null;
	}
	
	public Dataset<Order> getOrderList(Order.buys role, Condition<OrderAttribute> condition1, Condition<CustomerAttribute> condition2) {
		if(role != null) {
			if(role.equals(Order.buys.order))
				return getOrderListInBuys(condition1, condition2);
		}
		return null;
	}
	
	
	
	public Dataset<Order> getOrderList(Order.composed_of role, Product product) {
		if(role != null) {
			if(role.equals(Order.composed_of.orderP))
				return getOrderPListInComposed_ofByOrderedProducts(product);
		}
		return null;
	}
	
	public Dataset<Order> getOrderList(Order.composed_of role, Condition<ProductAttribute> condition) {
		if(role != null) {
			if(role.equals(Order.composed_of.orderP))
				return getOrderPListInComposed_ofByOrderedProductsCondition(condition);
		}
		return null;
	}
	
	public Dataset<Order> getOrderList(Order.composed_of role, Condition<OrderAttribute> condition1, Condition<ProductAttribute> condition2) {
		if(role != null) {
			if(role.equals(Order.composed_of.orderP))
				return getOrderPListInComposed_of(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	
	
	public abstract Dataset<Order> getOrderListInBuys(conditions.Condition<conditions.OrderAttribute> order_condition,conditions.Condition<conditions.CustomerAttribute> client_condition);
	
	public Dataset<Order> getOrderListInBuysByOrderCondition(conditions.Condition<conditions.OrderAttribute> order_condition){
		return getOrderListInBuys(order_condition, null);
	}
	public Dataset<Order> getOrderListInBuysByClientCondition(conditions.Condition<conditions.CustomerAttribute> client_condition){
		return getOrderListInBuys(null, client_condition);
	}
	
	public Dataset<Order> getOrderListInBuysByClient(pojo.Customer client){
		if(client == null)
			return null;
	
		Condition c;
		c=Condition.simple(CustomerAttribute.id,Operator.EQUALS, client.getId());
		Dataset<Order> res = getOrderListInBuysByClientCondition(c);
		return res;
	}
	
	public abstract Dataset<Order> getOrderPListInComposed_of(conditions.Condition<conditions.OrderAttribute> orderP_condition,conditions.Condition<conditions.ProductAttribute> orderedProducts_condition);
	
	public Dataset<Order> getOrderPListInComposed_ofByOrderPCondition(conditions.Condition<conditions.OrderAttribute> orderP_condition){
		return getOrderPListInComposed_of(orderP_condition, null);
	}
	public Dataset<Order> getOrderPListInComposed_ofByOrderedProductsCondition(conditions.Condition<conditions.ProductAttribute> orderedProducts_condition){
		return getOrderPListInComposed_of(null, orderedProducts_condition);
	}
	
	public Dataset<Order> getOrderPListInComposed_ofByOrderedProducts(pojo.Product orderedProducts){
		if(orderedProducts == null)
			return null;
	
		Condition c;
		c=Condition.simple(ProductAttribute.id,Operator.EQUALS, orderedProducts.getId());
		Dataset<Order> res = getOrderPListInComposed_ofByOrderedProductsCondition(c);
		return res;
	}
	
	
	public abstract boolean insertOrder(
		Order order,
		Customer	clientBuys,
		 List<Product> orderedProductsComposed_of);
	
	public abstract boolean insertOrderInOrdersColFromMongobench(Order order,
		Customer	clientBuys,
		 List<Product> orderedProductsComposed_of);
	public abstract void updateOrderList(conditions.Condition<conditions.OrderAttribute> condition, conditions.SetClause<conditions.OrderAttribute> set);
	
	public void updateOrder(pojo.Order order) {
		//TODO using the id
		return;
	}
	public abstract void updateOrderListInBuys(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.CustomerAttribute> client_condition,
		
		conditions.SetClause<conditions.OrderAttribute> set
	);
	
	public void updateOrderListInBuysByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInBuys(order_condition, null, set);
	}
	public void updateOrderListInBuysByClientCondition(
		conditions.Condition<conditions.CustomerAttribute> client_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderListInBuys(null, client_condition, set);
	}
	
	public void updateOrderListInBuysByClient(
		pojo.Customer client,
		conditions.SetClause<conditions.OrderAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void updateOrderPListInComposed_of(
		conditions.Condition<conditions.OrderAttribute> orderP_condition,
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition,
		
		conditions.SetClause<conditions.OrderAttribute> set
	);
	
	public void updateOrderPListInComposed_ofByOrderPCondition(
		conditions.Condition<conditions.OrderAttribute> orderP_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderPListInComposed_of(orderP_condition, null, set);
	}
	public void updateOrderPListInComposed_ofByOrderedProductsCondition(
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition,
		conditions.SetClause<conditions.OrderAttribute> set
	){
		updateOrderPListInComposed_of(null, orderedProducts_condition, set);
	}
	
	public void updateOrderPListInComposed_ofByOrderedProducts(
		pojo.Product orderedProducts,
		conditions.SetClause<conditions.OrderAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	
	
	public abstract void deleteOrderList(conditions.Condition<conditions.OrderAttribute> condition);
	
	public void deleteOrder(pojo.Order order) {
		//TODO using the id
		return;
	}
	public abstract void deleteOrderListInBuys(	
		conditions.Condition<conditions.OrderAttribute> order_condition,	
		conditions.Condition<conditions.CustomerAttribute> client_condition);
	
	public void deleteOrderListInBuysByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteOrderListInBuys(order_condition, null);
	}
	public void deleteOrderListInBuysByClientCondition(
		conditions.Condition<conditions.CustomerAttribute> client_condition
	){
		deleteOrderListInBuys(null, client_condition);
	}
	
	public void deleteOrderListInBuysByClient(
		pojo.Customer client 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void deleteOrderPListInComposed_of(	
		conditions.Condition<conditions.OrderAttribute> orderP_condition,	
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition);
	
	public void deleteOrderPListInComposed_ofByOrderPCondition(
		conditions.Condition<conditions.OrderAttribute> orderP_condition
	){
		deleteOrderPListInComposed_of(orderP_condition, null);
	}
	public void deleteOrderPListInComposed_ofByOrderedProductsCondition(
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition
	){
		deleteOrderPListInComposed_of(null, orderedProducts_condition);
	}
	
	public void deleteOrderPListInComposed_ofByOrderedProducts(
		pojo.Product orderedProducts 
	){
		//TODO get id in condition
		return;	
	}
	
	
}
