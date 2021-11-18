package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Product;
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
import conditions.ProductAttribute;
import conditions.CustomerAttribute;
import pojo.Customer;
import conditions.ProductAttribute;
import conditions.OrderAttribute;
import pojo.Order;

public abstract class ProductService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProductService.class);
	protected FeedbackService feedbackService = new dao.impl.FeedbackServiceImpl();
	protected Composed_ofService composed_ofService = new dao.impl.Composed_ofServiceImpl();
	


	public static enum ROLE_NAME {
		FEEDBACK_REVIEWEDPRODUCT, COMPOSED_OF_ORDEREDPRODUCTS
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.FEEDBACK_REVIEWEDPRODUCT, loading.Loading.LAZY);
		defaultLoadingParameters.put(ROLE_NAME.COMPOSED_OF_ORDEREDPRODUCTS, loading.Loading.LAZY);
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
	
	
	public Dataset<Product> getProductList(){
		return getProductList(null);
	}
	
	public Dataset<Product> getProductList(conditions.Condition<conditions.ProductAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Product>> datasets = new ArrayList<Dataset<Product>>();
		Dataset<Product> d = null;
		d = getProductListInProductsFromRedisModelB(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsProduct(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Product>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"id"});
		return d;
	}
	
	
	
	
	public abstract Dataset<Product> getProductListInProductsFromRedisModelB(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag);
	
	
	public Product getProductById(String id){
		Condition cond;
		cond = Condition.simple(ProductAttribute.id, conditions.Operator.EQUALS, id);
		Dataset<Product> res = getProductList(cond);
		if(res!=null && !res.isEmpty())
			return res.first();
		return null;
	}
	
	public Dataset<Product> getProductListById(String id) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Product> getProductListByTitle(String title) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.title, conditions.Operator.EQUALS, title));
	}
	
	public Dataset<Product> getProductListByPrice(Double price) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.price, conditions.Operator.EQUALS, price));
	}
	
	public Dataset<Product> getProductListByPhoto(String photo) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.photo, conditions.Operator.EQUALS, photo));
	}
	
	
	
	public static Dataset<Product> fullOuterJoinsProduct(List<Dataset<Product>> datasetsPOJO) {
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
								.withColumnRenamed("title", "title_1")
								.withColumnRenamed("price", "price_1")
								.withColumnRenamed("photo", "photo_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("title", "title_" + i)
								.withColumnRenamed("price", "price_" + i)
								.withColumnRenamed("photo", "photo_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			} 
			d = res.map((MapFunction<Row, Product>) r -> {
					Product product_res = new Product();
					
					// attribute 'Product.id'
					String firstNotNull_id = Util.getStringValue(r.getAs("id"));
					product_res.setId(firstNotNull_id);
					
					// attribute 'Product.title'
					String firstNotNull_title = Util.getStringValue(r.getAs("title"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String title2 = Util.getStringValue(r.getAs("title_" + i));
						if (firstNotNull_title != null && title2 != null && !firstNotNull_title.equals(title2)) {
							product_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.title': " + firstNotNull_title + " and " + title2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.title': " + firstNotNull_title + " and " + title2 + "." );
						}
						if (firstNotNull_title == null && title2 != null) {
							firstNotNull_title = title2;
						}
					}
					product_res.setTitle(firstNotNull_title);
					
					// attribute 'Product.price'
					Double firstNotNull_price = Util.getDoubleValue(r.getAs("price"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double price2 = Util.getDoubleValue(r.getAs("price_" + i));
						if (firstNotNull_price != null && price2 != null && !firstNotNull_price.equals(price2)) {
							product_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.price': " + firstNotNull_price + " and " + price2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.price': " + firstNotNull_price + " and " + price2 + "." );
						}
						if (firstNotNull_price == null && price2 != null) {
							firstNotNull_price = price2;
						}
					}
					product_res.setPrice(firstNotNull_price);
					
					// attribute 'Product.photo'
					String firstNotNull_photo = Util.getStringValue(r.getAs("photo"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String photo2 = Util.getStringValue(r.getAs("photo_" + i));
						if (firstNotNull_photo != null && photo2 != null && !firstNotNull_photo.equals(photo2)) {
							product_res.addLogEvent("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.photo': " + firstNotNull_photo + " and " + photo2 + "." );
							logger.warn("Data consistency problem for [Product - id :"+product_res.getId()+"]: different values found for attribute 'Product.photo': " + firstNotNull_photo + " and " + photo2 + "." );
						}
						if (firstNotNull_photo == null && photo2 != null) {
							firstNotNull_photo = photo2;
						}
					}
					product_res.setPhoto(firstNotNull_photo);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							product_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							product_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return product_res;
				}, Encoders.bean(Product.class));
			return d;
	}
	
	
	
	public Dataset<Product> getProductList(Product.composed_of role, Order order) {
		if(role != null) {
			if(role.equals(Product.composed_of.orderedProducts))
				return getOrderedProductsListInComposed_ofByOrderP(order);
		}
		return null;
	}
	
	public Dataset<Product> getProductList(Product.composed_of role, Condition<OrderAttribute> condition) {
		if(role != null) {
			if(role.equals(Product.composed_of.orderedProducts))
				return getOrderedProductsListInComposed_ofByOrderPCondition(condition);
		}
		return null;
	}
	
	public Dataset<Product> getProductList(Product.composed_of role, Condition<OrderAttribute> condition1, Condition<ProductAttribute> condition2) {
		if(role != null) {
			if(role.equals(Product.composed_of.orderedProducts))
				return getOrderedProductsListInComposed_of(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	public abstract Dataset<Product> getReviewedProductListInFeedback(conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,conditions.Condition<conditions.CustomerAttribute> reviewer_condition, conditions.Condition<conditions.FeedbackAttribute> feedback_condition);
	
	public Dataset<Product> getReviewedProductListInFeedbackByReviewedProductCondition(conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition){
		return getReviewedProductListInFeedback(reviewedProduct_condition, null, null);
	}
	public Dataset<Product> getReviewedProductListInFeedbackByReviewerCondition(conditions.Condition<conditions.CustomerAttribute> reviewer_condition){
		return getReviewedProductListInFeedback(null, reviewer_condition, null);
	}
	
	public Dataset<Product> getReviewedProductListInFeedbackByReviewer(pojo.Customer reviewer){
		if(reviewer == null)
			return null;
	
		Condition c;
		c=Condition.simple(CustomerAttribute.id,Operator.EQUALS, reviewer.getId());
		Dataset<Product> res = getReviewedProductListInFeedbackByReviewerCondition(c);
		return res;
	}
	
	public Dataset<Product> getReviewedProductListInFeedbackByFeedbackCondition(
		conditions.Condition<conditions.FeedbackAttribute> feedback_condition
	){
		return getReviewedProductListInFeedback(null, null, feedback_condition);
	}
	public abstract Dataset<Product> getOrderedProductsListInComposed_of(conditions.Condition<conditions.OrderAttribute> orderP_condition,conditions.Condition<conditions.ProductAttribute> orderedProducts_condition);
	
	public Dataset<Product> getOrderedProductsListInComposed_ofByOrderPCondition(conditions.Condition<conditions.OrderAttribute> orderP_condition){
		return getOrderedProductsListInComposed_of(orderP_condition, null);
	}
	
	public Dataset<Product> getOrderedProductsListInComposed_ofByOrderP(pojo.Order orderP){
		if(orderP == null)
			return null;
	
		Condition c;
		c=Condition.simple(OrderAttribute.id,Operator.EQUALS, orderP.getId());
		Dataset<Product> res = getOrderedProductsListInComposed_ofByOrderPCondition(c);
		return res;
	}
	
	public Dataset<Product> getOrderedProductsListInComposed_ofByOrderedProductsCondition(conditions.Condition<conditions.ProductAttribute> orderedProducts_condition){
		return getOrderedProductsListInComposed_of(null, orderedProducts_condition);
	}
	
	
	public abstract boolean insertProduct(Product product);
	
	public abstract boolean insertProductInProductsFromRedisModelB(Product product); 
	public abstract void updateProductList(conditions.Condition<conditions.ProductAttribute> condition, conditions.SetClause<conditions.ProductAttribute> set);
	
	public void updateProduct(pojo.Product product) {
		//TODO using the id
		return;
	}
	public abstract void updateReviewedProductListInFeedback(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		conditions.Condition<conditions.FeedbackAttribute> feedback,
		conditions.SetClause<conditions.ProductAttribute> set
	);
	
	public void updateReviewedProductListInFeedbackByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateReviewedProductListInFeedback(reviewedProduct_condition, null, null, set);
	}
	public void updateReviewedProductListInFeedbackByReviewerCondition(
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateReviewedProductListInFeedback(null, reviewer_condition, null, set);
	}
	
	public void updateReviewedProductListInFeedbackByReviewer(
		pojo.Customer reviewer,
		conditions.SetClause<conditions.ProductAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateReviewedProductListInFeedbackByFeedbackCondition(
		conditions.Condition<conditions.FeedbackAttribute> feedback_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateReviewedProductListInFeedback(null, null, feedback_condition, set);
	}
	public abstract void updateOrderedProductsListInComposed_of(
		conditions.Condition<conditions.OrderAttribute> orderP_condition,
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition,
		
		conditions.SetClause<conditions.ProductAttribute> set
	);
	
	public void updateOrderedProductsListInComposed_ofByOrderPCondition(
		conditions.Condition<conditions.OrderAttribute> orderP_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateOrderedProductsListInComposed_of(orderP_condition, null, set);
	}
	
	public void updateOrderedProductsListInComposed_ofByOrderP(
		pojo.Order orderP,
		conditions.SetClause<conditions.ProductAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateOrderedProductsListInComposed_ofByOrderedProductsCondition(
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateOrderedProductsListInComposed_of(null, orderedProducts_condition, set);
	}
	
	
	public abstract void deleteProductList(conditions.Condition<conditions.ProductAttribute> condition);
	
	public void deleteProduct(pojo.Product product) {
		//TODO using the id
		return;
	}
	public abstract void deleteReviewedProductListInFeedback(	
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,	
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		conditions.Condition<conditions.FeedbackAttribute> feedback);
	
	public void deleteReviewedProductListInFeedbackByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition
	){
		deleteReviewedProductListInFeedback(reviewedProduct_condition, null, null);
	}
	public void deleteReviewedProductListInFeedbackByReviewerCondition(
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition
	){
		deleteReviewedProductListInFeedback(null, reviewer_condition, null);
	}
	
	public void deleteReviewedProductListInFeedbackByReviewer(
		pojo.Customer reviewer 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteReviewedProductListInFeedbackByFeedbackCondition(
		conditions.Condition<conditions.FeedbackAttribute> feedback_condition
	){
		deleteReviewedProductListInFeedback(null, null, feedback_condition);
	}
	public abstract void deleteOrderedProductsListInComposed_of(	
		conditions.Condition<conditions.OrderAttribute> orderP_condition,	
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition);
	
	public void deleteOrderedProductsListInComposed_ofByOrderPCondition(
		conditions.Condition<conditions.OrderAttribute> orderP_condition
	){
		deleteOrderedProductsListInComposed_of(orderP_condition, null);
	}
	
	public void deleteOrderedProductsListInComposed_ofByOrderP(
		pojo.Order orderP 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteOrderedProductsListInComposed_ofByOrderedProductsCondition(
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition
	){
		deleteOrderedProductsListInComposed_of(null, orderedProducts_condition);
	}
	
}
