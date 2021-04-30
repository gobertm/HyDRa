package dao.services;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pojo.Product;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.commons.lang.mutable.MutableBoolean;
import conditions.Condition;
import conditions.Operator;
import conditions.ProductAttribute;
import conditions.OrderAttribute;
import pojo.Order;

public abstract class ProductService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProductService.class);
	protected OfService ofService = new dao.impl.OfServiceImpl();
	


	public static enum ROLE_NAME {
		OF_BOUGHT_ITEM
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.OF_BOUGHT_ITEM, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public ProductService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public ProductService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<Product> getProductList(conditions.Condition<conditions.ProductAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Product>> datasets = new ArrayList<Dataset<Product>>();
		Dataset<Product> d = null;
		d = getProductListInPRODUCTFromINVENTORY(condition, refilterFlag);
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
								.withColumnRenamed("label", "label_1")
								.withColumnRenamed("price", "price_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, "fullouter");
			for(int i = 2; i < datasets.size(); i++) {
				res = res.join(datasets.get(i)
								.withColumnRenamed("label", "label_" + i)
								.withColumnRenamed("price", "price_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
							, seq, "fullouter");
			} 
			d = res.map((MapFunction<Row, Product>) r -> {
					Product product_res = new Product();
					
					// attribute 'Product.id'
					Integer firstNotNull_id = r.getAs("id");
					product_res.setId(firstNotNull_id);
					
					// attribute 'Product.label'
					String firstNotNull_label = r.getAs("label");
					for (int i = 1; i < datasets.size(); i++) {
						String label2 = r.getAs("label_" + i);
						if (firstNotNull_label != null && label2 != null && !firstNotNull_label.equals(label2)) {
							product_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Product.label': " + firstNotNull_label + " and " + label2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Product.label' ==> " + firstNotNull_label + " and " + label2);
						}
						if (firstNotNull_label == null && label2 != null) {
							firstNotNull_label = label2;
						}
					}
					product_res.setLabel(firstNotNull_label);
					
					// attribute 'Product.price'
					Double firstNotNull_price = r.getAs("price");
					for (int i = 1; i < datasets.size(); i++) {
						Double price2 = r.getAs("price_" + i);
						if (firstNotNull_price != null && price2 != null && !firstNotNull_price.equals(price2)) {
							product_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Product.price': " + firstNotNull_price + " and " + price2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Product.price' ==> " + firstNotNull_price + " and " + price2);
						}
						if (firstNotNull_price == null && price2 != null) {
							firstNotNull_price = price2;
						}
					}
					product_res.setPrice(firstNotNull_price);
					
					scala.collection.mutable.WrappedArray<String> logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							product_res.addLogEvent(logEvents.apply(i));
						}
		
					for (int i = 1; i < datasets.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							product_res.addLogEvent(logEvents.apply(j));
						}
					}
					
					return product_res;
				}, Encoders.bean(Product.class));
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Product>) r -> condition == null || condition.evaluate(r));
		d=d.dropDuplicates();
		return d;
	}
	
	
	
	public abstract Dataset<Product> getProductListInPRODUCTFromINVENTORY(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag);
	
	
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
	
	
	
	protected static Dataset<Product> fullOuterJoinsProduct(List<Dataset<Product>> datasetsPOJO) {
		return fullOuterJoinsProduct(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Product> fullLeftOuterJoinsProduct(List<Dataset<Product>> datasetsPOJO) {
		return fullOuterJoinsProduct(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Product> fullOuterJoinsProduct(List<Dataset<Product>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Product> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("label", "label_1")
								.withColumnRenamed("price", "price_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("label", "label_" + i)
								.withColumnRenamed("price", "price_" + i)
							, seq, joinMode);
			} 
			d = res.map((MapFunction<Row, Product>) r -> {
					Product product_res = new Product();
					
					// attribute 'Product.id'
					Integer firstNotNull_id = r.getAs("id");
					product_res.setId(firstNotNull_id);
					
					// attribute 'Product.label'
					String firstNotNull_label = r.getAs("label");
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String label2 = r.getAs("label_" + i);
						if (firstNotNull_label != null && label2 != null && !firstNotNull_label.equals(label2)) {
							product_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Product.label': " + firstNotNull_label + " and " + label2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Product.label' ==> " + firstNotNull_label + " and " + label2);
						}
						if (firstNotNull_label == null && label2 != null) {
							firstNotNull_label = label2;
						}
					}
					product_res.setLabel(firstNotNull_label);
					
					// attribute 'Product.price'
					Double firstNotNull_price = r.getAs("price");
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double price2 = r.getAs("price_" + i);
						if (firstNotNull_price != null && price2 != null && !firstNotNull_price.equals(price2)) {
							product_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Product.price': " + firstNotNull_price + " and " + price2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Product.price' ==> " + firstNotNull_price + " and " + price2);
						}
						if (firstNotNull_price == null && price2 != null) {
							firstNotNull_price = price2;
						}
					}
					product_res.setPrice(firstNotNull_price);
					return product_res;
				}, Encoders.bean(Product.class));
			return d;
	}
	
	
	
	public Product getProduct(Product.of role, Order order) {
		if(role != null) {
			if(role.equals(Product.of.bought_item))
				return getBought_itemInOfByOrder(order);
		}
		return null;
	}
	
	public Dataset<Product> getProductList(Product.of role, Condition<OrderAttribute> condition) {
		if(role != null) {
			if(role.equals(Product.of.bought_item))
				return getBought_itemListInOfByOrderCondition(condition);
		}
		return null;
	}
	
	public Dataset<Product> getProductList(Product.of role, Condition<ProductAttribute> condition1, Condition<OrderAttribute> condition2) {
		if(role != null) {
			if(role.equals(Product.of.bought_item))
				return getBought_itemListInOf(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	
	public abstract Dataset<Product> getBought_itemListInOf(conditions.Condition<conditions.ProductAttribute> bought_item_condition,conditions.Condition<conditions.OrderAttribute> order_condition);
	
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
		return res.first();
	}
	
	
	public abstract void insertProductAndLinkedItems(Product product);
	public abstract void insertProduct(Product product);
	
	public abstract void insertProductInPRODUCTFromINVENTORY(Product product); 
	public abstract void updateProductList(conditions.Condition<conditions.ProductAttribute> condition, conditions.SetClause<conditions.ProductAttribute> set);
	
	public void updateProduct(pojo.Product product) {
		//TODO using the id
		return;
	}
	public abstract void updateBought_itemListInOf(
		conditions.Condition<conditions.ProductAttribute> bought_item_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition,
		
		conditions.SetClause<conditions.ProductAttribute> set
	);
	
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
	
	
	
	public abstract void deleteProductList(conditions.Condition<conditions.ProductAttribute> condition);
	
	public void deleteProduct(pojo.Product product) {
		//TODO using the id
		return;
	}
	public abstract void deleteBought_itemListInOf(	
		conditions.Condition<conditions.ProductAttribute> bought_item_condition,	
		conditions.Condition<conditions.OrderAttribute> order_condition);
	
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
