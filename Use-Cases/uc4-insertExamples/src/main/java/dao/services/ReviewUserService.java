package dao.services;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import conditions.Condition;
import pojo.ReviewUser;
import tdo.UserTDO;
import tdo.ReviewUserTDO;
import pojo.User;
import pojo.ReviewUser;
import conditions.UserAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;
import tdo.ReviewTDO;
import tdo.ReviewUserTDO;
import pojo.Review;
import pojo.ReviewUser;
import conditions.ReviewAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;


public abstract class ReviewUserService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReviewUserService.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	
	
	// method accessing the embedded object author mapped to role r_review1
	public abstract Dataset<pojo.ReviewUser> getReviewUserListInIMDB_MongoreviewColauthor(Condition<ReviewAttribute> r_review1_condition, Condition<UserAttribute> r_author_condition, MutableBoolean r_review1_refilter, MutableBoolean r_author_refilter);
	
	
	
	public abstract java.util.List<pojo.ReviewUser> getReviewUserList(
		conditions.Condition<conditions.UserAttribute> r_author_condition,
		conditions.Condition<conditions.ReviewAttribute> r_review1_condition);
	
	public java.util.List<pojo.ReviewUser> getReviewUserListByR_authorCondition(
		conditions.Condition<conditions.UserAttribute> r_author_condition
	){
		return getReviewUserList(r_author_condition, null);
	}
	
	public java.util.List<pojo.ReviewUser> getReviewUserListByR_author(pojo.User r_author) {
		// TODO using id for selecting
		return null;
	}
	public java.util.List<pojo.ReviewUser> getReviewUserListByR_review1Condition(
		conditions.Condition<conditions.ReviewAttribute> r_review1_condition
	){
		return getReviewUserList(null, r_review1_condition);
	}
	
	public pojo.ReviewUser getReviewUserByR_review1(pojo.Review r_review1) {
		// TODO using id for selecting
		return null;
	}
	
	public abstract void insertReviewUser(ReviewUser reviewUser);
	
	public abstract void deleteReviewUserList(
		conditions.Condition<conditions.UserAttribute> r_author_condition,
		conditions.Condition<conditions.ReviewAttribute> r_review1_condition);
	
	public void deleteReviewUserListByR_authorCondition(
		conditions.Condition<conditions.UserAttribute> r_author_condition
	){
		deleteReviewUserList(r_author_condition, null);
	}
	
	public void deleteReviewUserListByR_author(pojo.User r_author) {
		// TODO using id for selecting
		return;
	}
	public void deleteReviewUserListByR_review1Condition(
		conditions.Condition<conditions.ReviewAttribute> r_review1_condition
	){
		deleteReviewUserList(null, r_review1_condition);
	}
	
	public void deleteReviewUserByR_review1(pojo.Review r_review1) {
		// TODO using id for selecting
		return;
	}
		
}
