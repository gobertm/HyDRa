package pojo;

public class From extends LoggingPojo {

	private Store store;	
	private Order order;	

	public From() {}

	@Override
    public Object clone() throws CloneNotSupportedException {
        From cloned = (From) super.clone();
		cloned.setStore((Store)cloned.getStore().clone());	
		cloned.setOrder((Order)cloned.getOrder().clone());	
		return cloned;
    }

	public Store getStore() {
		return store;
	}	

	public void setStore(Store store) {
		this.store = store;
	}
	
	public Order getOrder() {
		return order;
	}	

	public void setOrder(Order order) {
		this.order = order;
	}
	

}
