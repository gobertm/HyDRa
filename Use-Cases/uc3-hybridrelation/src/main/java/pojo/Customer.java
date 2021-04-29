package pojo;

public class Customer extends LoggingPojo {

	private Integer id;
	private String firstName;
	private String lastName;
	private String address;

	public enum places {
		buyer
	}
	private java.util.List<Order> orderList;

	
	public Customer() {}

	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
	

	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}
	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
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
