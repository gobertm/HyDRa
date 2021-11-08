package tdo;

import pojo.Feedback;
import java.util.List;

public class FeedbackTDO extends Feedback {
	private  String kvSchema_feedback_customer1_customerid;
	public  String getKvSchema_feedback_customer1_customerid() {
		return this.kvSchema_feedback_customer1_customerid;
	}

	public void setKvSchema_feedback_customer1_customerid(  String kvSchema_feedback_customer1_customerid) {
		this.kvSchema_feedback_customer1_customerid = kvSchema_feedback_customer1_customerid;
	}

	private  String kvSchema_feedback_product_prodid;
	public  String getKvSchema_feedback_product_prodid() {
		return this.kvSchema_feedback_product_prodid;
	}

	public void setKvSchema_feedback_product_prodid(  String kvSchema_feedback_product_prodid) {
		this.kvSchema_feedback_product_prodid = kvSchema_feedback_product_prodid;
	}

}
