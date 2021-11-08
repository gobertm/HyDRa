package dao.services;

import util.Dataset;
import util.Row;
import conditions.Condition;
import pojo.Write;
import java.time.LocalDate;
import tdo.FeedbackTDO;
import tdo.WriteTDO;
import pojo.Feedback;
import pojo.Write;
import conditions.FeedbackAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;
import java.util.List;
import tdo.CustomerTDO;
import tdo.WriteTDO;
import pojo.Customer;
import pojo.Write;
import conditions.CustomerAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;
import java.util.List;


public abstract class WriteService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WriteService.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	
	
	// Left side 'customerid' of reference [customer1 ]
	public abstract Dataset<FeedbackTDO> getFeedbackTDOListReviewInCustomer1InFeedbackFromKvSchema(Condition<FeedbackAttribute> condition, MutableBoolean refilterFlag);
	
	// Right side 'id' of reference [customer1 ]
	public abstract Dataset<CustomerTDO> getCustomerTDOListReviewerInCustomer1InFeedbackFromKvSchema(Condition<CustomerAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	
	
	public abstract java.util.List<pojo.Write> getWriteList(
		conditions.Condition<conditions.FeedbackAttribute> review_condition,
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition);
	
	public java.util.List<pojo.Write> getWriteListByReviewCondition(
		conditions.Condition<conditions.FeedbackAttribute> review_condition
	){
		return getWriteList(review_condition, null);
	}
	
	public java.util.List<pojo.Write> getWriteListByReview(pojo.Feedback review) {
		// TODO using id for selecting
		return null;
	}
	public java.util.List<pojo.Write> getWriteListByReviewerCondition(
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition
	){
		return getWriteList(null, reviewer_condition);
	}
	
	public java.util.List<pojo.Write> getWriteListByReviewer(pojo.Customer reviewer) {
		// TODO using id for selecting
		return null;
	}
	
	public abstract void insertWrite(Write write);
	
	
	
	public 	abstract boolean insertWriteInRefStructFeedbackInRedisbench(Write write);
	
	 public void insertWrite(Feedback review ,Customer reviewer ){
		Write write = new Write();
		write.setReview(review);
		write.setReviewer(reviewer);
		insertWrite(write);
	}
	
	 public void insertWrite(Customer customer, List<Feedback> reviewList){
		Write write = new Write();
		write.setReviewer(customer);
		for(Feedback review : reviewList){
			write.setReview(review);
			insertWrite(write);
		}
	}
	 public void insertWrite(Feedback feedback, List<Customer> reviewerList){
		Write write = new Write();
		write.setReview(feedback);
		for(Customer reviewer : reviewerList){
			write.setReviewer(reviewer);
			insertWrite(write);
		}
	}
	
	
	public abstract void deleteWriteList(
		conditions.Condition<conditions.FeedbackAttribute> review_condition,
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition);
	
	public void deleteWriteListByReviewCondition(
		conditions.Condition<conditions.FeedbackAttribute> review_condition
	){
		deleteWriteList(review_condition, null);
	}
	
	public void deleteWriteListByReview(pojo.Feedback review) {
		// TODO using id for selecting
		return;
	}
	public void deleteWriteListByReviewerCondition(
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition
	){
		deleteWriteList(null, reviewer_condition);
	}
	
	public void deleteWriteListByReviewer(pojo.Customer reviewer) {
		// TODO using id for selecting
		return;
	}
		
}
