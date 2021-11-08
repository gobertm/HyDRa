package tdo;

import pojo.Order;
import java.util.List;

public class OrderTDO extends Order {
	private  String docSchema_ordersCol_customer_PersonId;
	public  String getDocSchema_ordersCol_customer_PersonId() {
		return this.docSchema_ordersCol_customer_PersonId;
	}

	public void setDocSchema_ordersCol_customer_PersonId(  String docSchema_ordersCol_customer_PersonId) {
		this.docSchema_ordersCol_customer_PersonId = docSchema_ordersCol_customer_PersonId;
	}

}
