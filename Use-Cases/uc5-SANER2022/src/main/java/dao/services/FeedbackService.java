package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Feedback;
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
import conditions.FeedbackAttribute;
import conditions.CustomerAttribute;
import pojo.Customer;
import conditions.FeedbackAttribute;
import conditions.ProductAttribute;
import pojo.Product;

public abstract class FeedbackService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FeedbackService.class);
	protected WriteService writeService = new dao.impl.WriteServiceImpl();
	protected Has_reviewsService has_reviewsService = new dao.impl.Has_reviewsServiceImpl();
	


	public static enum ROLE_NAME {
		WRITE_REVIEW, HAS_REVIEWS_REVIEWS
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.WRITE_REVIEW, loading.Loading.LAZY);
		defaultLoadingParameters.put(ROLE_NAME.HAS_REVIEWS_REVIEWS, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public FeedbackService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public FeedbackService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<Feedback> getFeedbackList(){
		return getFeedbackList(null);
	}
	
	public Dataset<Feedback> getFeedbackList(conditions.Condition<conditions.FeedbackAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Feedback>> datasets = new ArrayList<Dataset<Feedback>>();
		Dataset<Feedback> d = null;
		d = getFeedbackListInFeedbackFromRedisbench(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsFeedback(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Feedback>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates();
		return d;
	}
	
	
	
	
	public abstract Dataset<Feedback> getFeedbackListInFeedbackFromRedisbench(conditions.Condition<conditions.FeedbackAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	public Dataset<Feedback> getFeedbackListByRate(Double rate) {
		return getFeedbackList(conditions.Condition.simple(conditions.FeedbackAttribute.rate, conditions.Operator.EQUALS, rate));
	}
	
	public Dataset<Feedback> getFeedbackListByContent(String content) {
		return getFeedbackList(conditions.Condition.simple(conditions.FeedbackAttribute.content, conditions.Operator.EQUALS, content));
	}
	
	public Dataset<Feedback> getFeedbackListByProduct(String product) {
		return getFeedbackList(conditions.Condition.simple(conditions.FeedbackAttribute.product, conditions.Operator.EQUALS, product));
	}
	
	public Dataset<Feedback> getFeedbackListByCustomer(String customer) {
		return getFeedbackList(conditions.Condition.simple(conditions.FeedbackAttribute.customer, conditions.Operator.EQUALS, customer));
	}
	
	
	
	protected static Dataset<Feedback> fullOuterJoinsFeedback(List<Dataset<Feedback>> datasetsPOJO) {
		return fullOuterJoinsFeedback(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Feedback> fullLeftOuterJoinsFeedback(List<Dataset<Feedback>> datasetsPOJO) {
		return fullOuterJoinsFeedback(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Feedback> fullOuterJoinsFeedback(List<Dataset<Feedback>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Feedback> d = datasetsPOJO.get(0);
			for(int i = 1; i < datasetsPOJO.size(); i++)
				d = d.union(datasetsPOJO.get(i));
			return d;
	}
	
	
	
	
	public Dataset<Feedback> getFeedbackList(Feedback.write role, Customer customer) {
		if(role != null) {
			if(role.equals(Feedback.write.review))
				return getReviewListInWriteByReviewer(customer);
		}
		return null;
	}
	
	public Dataset<Feedback> getFeedbackList(Feedback.write role, Condition<CustomerAttribute> condition) {
		if(role != null) {
			if(role.equals(Feedback.write.review))
				return getReviewListInWriteByReviewerCondition(condition);
		}
		return null;
	}
	
	public Dataset<Feedback> getFeedbackList(Feedback.write role, Condition<FeedbackAttribute> condition1, Condition<CustomerAttribute> condition2) {
		if(role != null) {
			if(role.equals(Feedback.write.review))
				return getReviewListInWrite(condition1, condition2);
		}
		return null;
	}
	
	
	
	public Dataset<Feedback> getFeedbackList(Feedback.has_reviews role, Product product) {
		if(role != null) {
			if(role.equals(Feedback.has_reviews.reviews))
				return getReviewsListInHas_reviewsByReviewedProduct(product);
		}
		return null;
	}
	
	public Dataset<Feedback> getFeedbackList(Feedback.has_reviews role, Condition<ProductAttribute> condition) {
		if(role != null) {
			if(role.equals(Feedback.has_reviews.reviews))
				return getReviewsListInHas_reviewsByReviewedProductCondition(condition);
		}
		return null;
	}
	
	public Dataset<Feedback> getFeedbackList(Feedback.has_reviews role, Condition<FeedbackAttribute> condition1, Condition<ProductAttribute> condition2) {
		if(role != null) {
			if(role.equals(Feedback.has_reviews.reviews))
				return getReviewsListInHas_reviews(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	public abstract Dataset<Feedback> getReviewListInWrite(conditions.Condition<conditions.FeedbackAttribute> review_condition,conditions.Condition<conditions.CustomerAttribute> reviewer_condition);
	
	public Dataset<Feedback> getReviewListInWriteByReviewCondition(conditions.Condition<conditions.FeedbackAttribute> review_condition){
		return getReviewListInWrite(review_condition, null);
	}
	public Dataset<Feedback> getReviewListInWriteByReviewerCondition(conditions.Condition<conditions.CustomerAttribute> reviewer_condition){
		return getReviewListInWrite(null, reviewer_condition);
	}
	
	public Dataset<Feedback> getReviewListInWriteByReviewer(pojo.Customer reviewer){
		if(reviewer == null)
			return null;
	
		Condition c;
		c=Condition.simple(CustomerAttribute.id,Operator.EQUALS, reviewer.getId());
		Dataset<Feedback> res = getReviewListInWriteByReviewerCondition(c);
		return res;
	}
	
	public abstract Dataset<Feedback> getReviewsListInHas_reviews(conditions.Condition<conditions.FeedbackAttribute> reviews_condition,conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition);
	
	public Dataset<Feedback> getReviewsListInHas_reviewsByReviewsCondition(conditions.Condition<conditions.FeedbackAttribute> reviews_condition){
		return getReviewsListInHas_reviews(reviews_condition, null);
	}
	public Dataset<Feedback> getReviewsListInHas_reviewsByReviewedProductCondition(conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition){
		return getReviewsListInHas_reviews(null, reviewedProduct_condition);
	}
	
	public Dataset<Feedback> getReviewsListInHas_reviewsByReviewedProduct(pojo.Product reviewedProduct){
		if(reviewedProduct == null)
			return null;
	
		Condition c;
		c=Condition.simple(ProductAttribute.id,Operator.EQUALS, reviewedProduct.getId());
		Dataset<Feedback> res = getReviewsListInHas_reviewsByReviewedProductCondition(c);
		return res;
	}
	
	
	
	public abstract boolean insertFeedback(Feedback feedback);
	
	public abstract boolean insertFeedbackInFeedbackFromRedisbench(Feedback feedback); 
	public abstract void updateFeedbackList(conditions.Condition<conditions.FeedbackAttribute> condition, conditions.SetClause<conditions.FeedbackAttribute> set);
	
	public void updateFeedback(pojo.Feedback feedback) {
		//TODO using the id
		return;
	}
	public abstract void updateReviewListInWrite(
		conditions.Condition<conditions.FeedbackAttribute> review_condition,
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		
		conditions.SetClause<conditions.FeedbackAttribute> set
	);
	
	public void updateReviewListInWriteByReviewCondition(
		conditions.Condition<conditions.FeedbackAttribute> review_condition,
		conditions.SetClause<conditions.FeedbackAttribute> set
	){
		updateReviewListInWrite(review_condition, null, set);
	}
	public void updateReviewListInWriteByReviewerCondition(
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		conditions.SetClause<conditions.FeedbackAttribute> set
	){
		updateReviewListInWrite(null, reviewer_condition, set);
	}
	
	public void updateReviewListInWriteByReviewer(
		pojo.Customer reviewer,
		conditions.SetClause<conditions.FeedbackAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void updateReviewsListInHas_reviews(
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition,
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,
		
		conditions.SetClause<conditions.FeedbackAttribute> set
	);
	
	public void updateReviewsListInHas_reviewsByReviewsCondition(
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition,
		conditions.SetClause<conditions.FeedbackAttribute> set
	){
		updateReviewsListInHas_reviews(reviews_condition, null, set);
	}
	public void updateReviewsListInHas_reviewsByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,
		conditions.SetClause<conditions.FeedbackAttribute> set
	){
		updateReviewsListInHas_reviews(null, reviewedProduct_condition, set);
	}
	
	public void updateReviewsListInHas_reviewsByReviewedProduct(
		pojo.Product reviewedProduct,
		conditions.SetClause<conditions.FeedbackAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	
	
	public abstract void deleteFeedbackList(conditions.Condition<conditions.FeedbackAttribute> condition);
	
	public void deleteFeedback(pojo.Feedback feedback) {
		//TODO using the id
		return;
	}
	public abstract void deleteReviewListInWrite(	
		conditions.Condition<conditions.FeedbackAttribute> review_condition,	
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition);
	
	public void deleteReviewListInWriteByReviewCondition(
		conditions.Condition<conditions.FeedbackAttribute> review_condition
	){
		deleteReviewListInWrite(review_condition, null);
	}
	public void deleteReviewListInWriteByReviewerCondition(
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition
	){
		deleteReviewListInWrite(null, reviewer_condition);
	}
	
	public void deleteReviewListInWriteByReviewer(
		pojo.Customer reviewer 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void deleteReviewsListInHas_reviews(	
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition,	
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition);
	
	public void deleteReviewsListInHas_reviewsByReviewsCondition(
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition
	){
		deleteReviewsListInHas_reviews(reviews_condition, null);
	}
	public void deleteReviewsListInHas_reviewsByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition
	){
		deleteReviewsListInHas_reviews(null, reviewedProduct_condition);
	}
	
	public void deleteReviewsListInHas_reviewsByReviewedProduct(
		pojo.Product reviewedProduct 
	){
		//TODO get id in condition
		return;	
	}
	
	
}
