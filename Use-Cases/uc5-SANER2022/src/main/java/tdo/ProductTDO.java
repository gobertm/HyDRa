package tdo;

import pojo.Product;
import java.util.List;

public class ProductTDO extends Product {
	private  String kvSchema_feedback_product_asin;
	public  String getKvSchema_feedback_product_asin() {
		return this.kvSchema_feedback_product_asin;
	}

	public void setKvSchema_feedback_product_asin(  String kvSchema_feedback_product_asin) {
		this.kvSchema_feedback_product_asin = kvSchema_feedback_product_asin;
	}

}
