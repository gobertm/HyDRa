package tdo;

import pojo.Product;

public class ProductTDO extends Product {
	private String mongoSchema_ClientCollection_buys_ID;
	public String getMongoSchema_ClientCollection_buys_ID() {
		return this.mongoSchema_ClientCollection_buys_ID;
	}

	public void setMongoSchema_ClientCollection_buys_ID( String mongoSchema_ClientCollection_buys_ID) {
		this.mongoSchema_ClientCollection_buys_ID = mongoSchema_ClientCollection_buys_ID;
	}

}
