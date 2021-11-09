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
import conditions.OrderAttribute;
import pojo.Order;
import conditions.ProductAttribute;
import conditions.FeedbackAttribute;
import pojo.Feedback;

public abstract class ProductService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProductService.class);
	protected Composed_ofService composed_ofService = new dao.impl.Composed_ofServiceImpl();
	protected Has_reviewsService has_reviewsService = new dao.impl.Has_reviewsServiceImpl();
	


	public static enum ROLE_NAME {
		COMPOSED_OF_ORDEREDPRODUCTS, HAS_REVIEWS_REVIEWEDPRODUCT
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.COMPOSED_OF_ORDEREDPRODUCTS, loading.Loading.LAZY);
		defaultLoadingParameters.put(ROLE_NAME.HAS_REVIEWS_REVIEWEDPRODUCT, loading.Loading.LAZY);
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
		d = getProductListInOrdersColFromMongobench(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		d = getProductListInProductTableFromMysqlbench(condition, refilterFlag);
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
	
	
	
	
	public abstract Dataset<Product> getProductListInOrdersColFromMongobench(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	public abstract Dataset<Product> getProductListInProductTableFromMysqlbench(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag);
	
	
	// TODO get based on id(s). Ex:public Client getClientById(Long id)
	public Product getProductById(String id){
		Condition cond;
		cond = Condition.simple(ProductAttribute.id, conditions.Operator.EQUALS, id);
		Dataset<Product> res = getProductList(cond);
		if(res!=null)
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
	
	
	
	
	public Dataset<Product> getProductList(Product.has_reviews role, Feedback feedback) {
		if(role != null) {
			if(role.equals(Product.has_reviews.reviewedProduct))
				return getReviewedProductListInHas_reviewsByReviews(feedback);
		}
		return null;
	}
	
	public Dataset<Product> getProductList(Product.has_reviews role, Condition<FeedbackAttribute> condition) {
		if(role != null) {
			if(role.equals(Product.has_reviews.reviewedProduct))
				return getReviewedProductListInHas_reviewsByReviewsCondition(condition);
		}
		return null;
	}
	
	public Dataset<Product> getProductList(Product.has_reviews role, Condition<FeedbackAttribute> condition1, Condition<ProductAttribute> condition2) {
		if(role != null) {
			if(role.equals(Product.has_reviews.reviewedProduct))
				return getReviewedProductListInHas_reviews(condition1, condition2);
		}
		return null;
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
	public abstract Dataset<Product> getReviewedProductListInHas_reviews(conditions.Condition<conditions.FeedbackAttribute> reviews_condition,conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition);
	
	public Dataset<Product> getReviewedProductListInHas_reviewsByReviewsCondition(conditions.Condition<conditions.FeedbackAttribute> reviews_condition){
		return getReviewedProductListInHas_reviews(reviews_condition, null);
	}
	
	public Dataset<Product> getReviewedProductListInHas_reviewsByReviews(pojo.Feedback reviews){
		if(reviews == null)
			return null;
	
		Condition c;
		c=null;
		Dataset<Product> res = getReviewedProductListInHas_reviewsByReviewsCondition(c);
		return res;
	}
	
	public Dataset<Product> getReviewedProductListInHas_reviewsByReviewedProductCondition(conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition){
		return getReviewedProductListInHas_reviews(null, reviewedProduct_condition);
	}
	
	
	public abstract boolean insertProduct(Product product);
	
	public abstract boolean insertProductInProductTableFromMysqlbench(Product product); 
	public abstract void updateProductList(conditions.Condition<conditions.ProductAttribute> condition, conditions.SetClause<conditions.ProductAttribute> set);
	
	public void updateProduct(pojo.Product product) {
		//TODO using the id
		return;
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
	public abstract void updateReviewedProductListInHas_reviews(
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition,
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,
		
		conditions.SetClause<conditions.ProductAttribute> set
	);
	
	public void updateReviewedProductListInHas_reviewsByReviewsCondition(
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateReviewedProductListInHas_reviews(reviews_condition, null, set);
	}
	
	public void updateReviewedProductListInHas_reviewsByReviews(
		pojo.Feedback reviews,
		conditions.SetClause<conditions.ProductAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateReviewedProductListInHas_reviewsByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,
		conditions.SetClause<conditions.ProductAttribute> set
	){
		updateReviewedProductListInHas_reviews(null, reviewedProduct_condition, set);
	}
	
	
	public abstract void deleteProductList(conditions.Condition<conditions.ProductAttribute> condition);
	
	public void deleteProduct(pojo.Product product) {
		//TODO using the id
		return;
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
	public abstract void deleteReviewedProductListInHas_reviews(	
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition,	
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition);
	
	public void deleteReviewedProductListInHas_reviewsByReviewsCondition(
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition
	){
		deleteReviewedProductListInHas_reviews(reviews_condition, null);
	}
	
	public void deleteReviewedProductListInHas_reviewsByReviews(
		pojo.Feedback reviews 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteReviewedProductListInHas_reviewsByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition
	){
		deleteReviewedProductListInHas_reviews(null, reviewedProduct_condition);
	}
	
}
