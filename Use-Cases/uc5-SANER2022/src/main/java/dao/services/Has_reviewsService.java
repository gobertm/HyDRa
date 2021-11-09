package dao.services;

import util.Dataset;
import util.Row;
import conditions.Condition;
import pojo.Has_reviews;
import java.time.LocalDate;
import tdo.FeedbackTDO;
import tdo.Has_reviewsTDO;
import pojo.Feedback;
import pojo.Has_reviews;
import conditions.FeedbackAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;
import java.util.List;
import tdo.ProductTDO;
import tdo.Has_reviewsTDO;
import pojo.Product;
import pojo.Has_reviews;
import conditions.ProductAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;
import java.util.List;


public abstract class Has_reviewsService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Has_reviewsService.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	
	
	// Left side 'prodid' of reference [product ]
	public abstract Dataset<FeedbackTDO> getFeedbackTDOListReviewsInProductInFeedbackFromKvSchema(Condition<FeedbackAttribute> condition, MutableBoolean refilterFlag);
	
	// Right side 'asin' of reference [product ]
	public abstract Dataset<ProductTDO> getProductTDOListReviewedProductInProductInFeedbackFromKvSchema(Condition<ProductAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	
	
	public abstract java.util.List<pojo.Has_reviews> getHas_reviewsList(
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition,
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition);
	
	public java.util.List<pojo.Has_reviews> getHas_reviewsListByReviewsCondition(
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition
	){
		return getHas_reviewsList(reviews_condition, null);
	}
	
	public java.util.List<pojo.Has_reviews> getHas_reviewsListByReviews(pojo.Feedback reviews) {
		// TODO using id for selecting
		return null;
	}
	public java.util.List<pojo.Has_reviews> getHas_reviewsListByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition
	){
		return getHas_reviewsList(null, reviewedProduct_condition);
	}
	
	public java.util.List<pojo.Has_reviews> getHas_reviewsListByReviewedProduct(pojo.Product reviewedProduct) {
		// TODO using id for selecting
		return null;
	}
	
	public abstract void insertHas_reviews(Has_reviews has_reviews);
	
	
	
	public 	abstract boolean insertHas_reviewsInRefStructFeedbackInRedisbench(Has_reviews has_reviews);
	
	 public void insertHas_reviews(Feedback reviews ,Product reviewedProduct ){
		Has_reviews has_reviews = new Has_reviews();
		has_reviews.setReviews(reviews);
		has_reviews.setReviewedProduct(reviewedProduct);
		insertHas_reviews(has_reviews);
	}
	
	 public void insertHas_reviews(Product product, List<Feedback> reviewsList){
		Has_reviews has_reviews = new Has_reviews();
		has_reviews.setReviewedProduct(product);
		for(Feedback reviews : reviewsList){
			has_reviews.setReviews(reviews);
			insertHas_reviews(has_reviews);
		}
	}
	 public void insertHas_reviews(Feedback feedback, List<Product> reviewedProductList){
		Has_reviews has_reviews = new Has_reviews();
		has_reviews.setReviews(feedback);
		for(Product reviewedProduct : reviewedProductList){
			has_reviews.setReviewedProduct(reviewedProduct);
			insertHas_reviews(has_reviews);
		}
	}
	
	
	public abstract void deleteHas_reviewsList(
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition,
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition);
	
	public void deleteHas_reviewsListByReviewsCondition(
		conditions.Condition<conditions.FeedbackAttribute> reviews_condition
	){
		deleteHas_reviewsList(reviews_condition, null);
	}
	
	public void deleteHas_reviewsListByReviews(pojo.Feedback reviews) {
		// TODO using id for selecting
		return;
	}
	public void deleteHas_reviewsListByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition
	){
		deleteHas_reviewsList(null, reviewedProduct_condition);
	}
	
	public void deleteHas_reviewsListByReviewedProduct(pojo.Product reviewedProduct) {
		// TODO using id for selecting
		return;
	}
		
}
