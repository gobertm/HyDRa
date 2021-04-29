package pojo;

public class Places extends LoggingPojo {

	private Customer buyer;	
	private Order order;	

	public Places() {}

	@Override
    public Object clone() throws CloneNotSupportedException {
        Places cloned = (Places) super.clone();
		cloned.setBuyer((Customer)cloned.getBuyer().clone());	
		cloned.setOrder((Order)cloned.getOrder().clone());	
		return cloned;
    }

	public Customer getBuyer() {
		return buyer;
	}	

	public void setBuyer(Customer buyer) {
		this.buyer = buyer;
	}
	
	public Order getOrder() {
		return order;
	}	

	public void setOrder(Order order) {
		this.order = order;
	}
	

}
