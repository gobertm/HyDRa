package tdo;

import pojo.Customer;
import java.util.List;

public class CustomerTDO extends Customer {
	private  String docSchema_ordersCol_customer_id;
	public  String getDocSchema_ordersCol_customer_id() {
		return this.docSchema_ordersCol_customer_id;
	}

	public void setDocSchema_ordersCol_customer_id(  String docSchema_ordersCol_customer_id) {
		this.docSchema_ordersCol_customer_id = docSchema_ordersCol_customer_id;
	}

	private  String kvSchema_feedback_customer1_id;
	public  String getKvSchema_feedback_customer1_id() {
		return this.kvSchema_feedback_customer1_id;
	}

	public void setKvSchema_feedback_customer1_id(  String kvSchema_feedback_customer1_id) {
		this.kvSchema_feedback_customer1_id = kvSchema_feedback_customer1_id;
	}

}
