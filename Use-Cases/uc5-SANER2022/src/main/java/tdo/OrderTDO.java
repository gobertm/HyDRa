package tdo;

import pojo.Order;
import java.util.List;

public class OrderTDO extends Order {
	private  String relSchemaB_orderTable_clientRef_customerId;
	public  String getRelSchemaB_orderTable_clientRef_customerId() {
		return this.relSchemaB_orderTable_clientRef_customerId;
	}

	public void setRelSchemaB_orderTable_clientRef_customerId(  String relSchemaB_orderTable_clientRef_customerId) {
		this.relSchemaB_orderTable_clientRef_customerId = relSchemaB_orderTable_clientRef_customerId;
	}

	private  String mongoSchemaB_detailOrderCol_orderRef_orderId;
	public  String getMongoSchemaB_detailOrderCol_orderRef_orderId() {
		return this.mongoSchemaB_detailOrderCol_orderRef_orderId;
	}

	public void setMongoSchemaB_detailOrderCol_orderRef_orderId(  String mongoSchemaB_detailOrderCol_orderRef_orderId) {
		this.mongoSchemaB_detailOrderCol_orderRef_orderId = mongoSchemaB_detailOrderCol_orderRef_orderId;
	}

}
