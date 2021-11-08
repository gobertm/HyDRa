package pojo;
import java.time.LocalDate;
import java.util.List;

public class Order extends LoggingPojo {

	private String id;
	private LocalDate orderdate;
	private Double totalprice;

	public enum buys {
		order
	}
	private Customer client;
	public enum composed_of {
		orderP
	}
	private List<Product> orderedProductsList;

	// Empty constructor
	public Order() {}

	/*
	* Constructor on simple attribute 
	*/
	public Order(String id,LocalDate orderdate,Double totalprice) {
		this.id = id;
		this.orderdate = orderdate;
		this.totalprice = totalprice;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
	
	@Override
	public String toString(){
		return "Order { " + "id="+id +", "+
					"orderdate="+orderdate +", "+
					"totalprice="+totalprice +"}"; 
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	public LocalDate getOrderdate() {
		return orderdate;
	}

	public void setOrderdate(LocalDate orderdate) {
		this.orderdate = orderdate;
	}
	public Double getTotalprice() {
		return totalprice;
	}

	public void setTotalprice(Double totalprice) {
		this.totalprice = totalprice;
	}

	

	public Customer _getClient() {
		return client;
	}

	public void _setClient(Customer client) {
		this.client = client;
	}
	public List<Product> _getOrderedProductsList() {
		return orderedProductsList;
	}

	public void _setOrderedProductsList(List<Product> orderedProductsList) {
		this.orderedProductsList = orderedProductsList;
	}
}
