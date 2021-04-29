package pojo;

public class Of extends LoggingPojo {

	private Product bought_item;	
	private Order order;	

	public Of() {}

	@Override
    public Object clone() throws CloneNotSupportedException {
        Of cloned = (Of) super.clone();
		cloned.setBought_item((Product)cloned.getBought_item().clone());	
		cloned.setOrder((Order)cloned.getOrder().clone());	
		return cloned;
    }

	public Product getBought_item() {
		return bought_item;
	}	

	public void setBought_item(Product bought_item) {
		this.bought_item = bought_item;
	}
	
	public Order getOrder() {
		return order;
	}	

	public void setOrder(Order order) {
		this.order = order;
	}
	

}
