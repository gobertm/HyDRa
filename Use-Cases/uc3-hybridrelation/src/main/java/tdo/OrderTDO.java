package tdo;

import pojo.Order;

public class OrderTDO extends Order {
	private String mongoSchema_ClientCollection_buys_productId;
	public String getMongoSchema_ClientCollection_buys_productId() {
		return this.mongoSchema_ClientCollection_buys_productId;
	}

	public void setMongoSchema_ClientCollection_buys_productId( String mongoSchema_ClientCollection_buys_productId) {
		this.mongoSchema_ClientCollection_buys_productId = mongoSchema_ClientCollection_buys_productId;
	}

	private String mongoSchema_ClientCollection_buys_in_storeId;
	public String getMongoSchema_ClientCollection_buys_in_storeId() {
		return this.mongoSchema_ClientCollection_buys_in_storeId;
	}

	public void setMongoSchema_ClientCollection_buys_in_storeId( String mongoSchema_ClientCollection_buys_in_storeId) {
		this.mongoSchema_ClientCollection_buys_in_storeId = mongoSchema_ClientCollection_buys_in_storeId;
	}

}
