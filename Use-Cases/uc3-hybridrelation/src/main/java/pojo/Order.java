package pojo;

public class Order extends LoggingPojo {

	private Integer id;
	private Integer quantity;

	public enum places {
		order
	}
	private Customer buyer;
	public enum of {
		order
	}
	private Product bought_item;
	public enum from {
		order
	}
	private Store store;

	
	public Order() {}

	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
	
	@Override
	public String toString(){
		return "Order { " + "id="+id +", "+
"quantity="+quantity +"}"; 
	}
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}
	public Integer getQuantity() {
		return quantity;
	}

	public void setQuantity(Integer quantity) {
		this.quantity = quantity;
	}

	

	public Customer _getBuyer() {
		return buyer;
	}

	public void _setBuyer(Customer buyer) {
		this.buyer = buyer;
	}
	public Product _getBought_item() {
		return bought_item;
	}

	public void _setBought_item(Product bought_item) {
		this.bought_item = bought_item;
	}
	public Store _getStore() {
		return store;
	}

	public void _setStore(Store store) {
		this.store = store;
	}
}
