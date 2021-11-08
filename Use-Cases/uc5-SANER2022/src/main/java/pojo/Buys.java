package pojo;

public class Buys extends LoggingPojo {

	private Order order;	
	private Customer client;	

	public Buys() {}

	@Override
    public Object clone() throws CloneNotSupportedException {
        Buys cloned = (Buys) super.clone();
		cloned.setOrder((Order)cloned.getOrder().clone());	
		cloned.setClient((Customer)cloned.getClient().clone());	
		return cloned;
    }

	public Order getOrder() {
		return order;
	}	

	public void setOrder(Order order) {
		this.order = order;
	}
	
	public Customer getClient() {
		return client;
	}	

	public void setClient(Customer client) {
		this.client = client;
	}
	

}
