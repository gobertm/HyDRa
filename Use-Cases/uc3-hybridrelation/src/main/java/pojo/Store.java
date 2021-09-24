package pojo;

public class Store extends LoggingPojo {

	private Integer id;
	private String VAT;
	private String address;

	public enum from {
		store
	}
	private java.util.List<Order> orderList;

	
	public Store() {}

	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
	
	@Override
	public String toString(){
		return "Store { " + "id="+id +", "+
"VAT="+VAT +", "+
"address="+address +"}"; 
	}
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}
	public String getVAT() {
		return VAT;
	}

	public void setVAT(String VAT) {
		this.VAT = VAT;
	}
	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	

	public java.util.List<Order> _getOrderList() {
		return orderList;
	}

	public void _setOrderList(java.util.List<Order> orderList) {
		this.orderList = orderList;
	}
}
