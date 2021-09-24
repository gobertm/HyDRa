package pojo;

public class Product extends LoggingPojo {

	private Integer id;
	private String label;
	private Double price;

	public enum of {
		bought_item
	}
	private java.util.List<Order> orderList;

	
	public Product() {}

	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
	
	@Override
	public String toString(){
		return "Product { " + "id="+id +", "+
"label="+label +", "+
"price="+price +"}"; 
	}
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}
	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}
	public Double getPrice() {
		return price;
	}

	public void setPrice(Double price) {
		this.price = price;
	}

	

	public java.util.List<Order> _getOrderList() {
		return orderList;
	}

	public void _setOrderList(java.util.List<Order> orderList) {
		this.orderList = orderList;
	}
}
