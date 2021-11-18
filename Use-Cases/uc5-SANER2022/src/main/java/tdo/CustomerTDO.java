package tdo;

import pojo.Customer;
import java.util.List;

public class CustomerTDO extends Customer {
	private  String relSchemaB_orderTable_clientRef_id;
	public  String getRelSchemaB_orderTable_clientRef_id() {
		return this.relSchemaB_orderTable_clientRef_id;
	}

	public void setRelSchemaB_orderTable_clientRef_id(  String relSchemaB_orderTable_clientRef_id) {
		this.relSchemaB_orderTable_clientRef_id = relSchemaB_orderTable_clientRef_id;
	}

}
