package pojo;
import java.time.LocalDate;
import java.util.List;

public class Feedback extends LoggingPojo {

	private Double rate;
	private String content;
	private String product;
	private String customer;

	public enum write {
		review
	}
	private List<Customer> reviewerList;
	public enum has_reviews {
		reviews
	}
	private List<Product> reviewedProductList;

	// Empty constructor
	public Feedback() {}

	/*
	* Constructor on simple attribute 
	*/
	public Feedback(Double rate,String content,String product,String customer) {
		this.rate = rate;
		this.content = content;
		this.product = product;
		this.customer = customer;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
	
	@Override
	public String toString(){
		return "Feedback { " + "rate="+rate +", "+
					"content="+content +", "+
					"product="+product +", "+
					"customer="+customer +"}"; 
	}
	
	public Double getRate() {
		return rate;
	}

	public void setRate(Double rate) {
		this.rate = rate;
	}
	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}
	public String getProduct() {
		return product;
	}

	public void setProduct(String product) {
		this.product = product;
	}
	public String getCustomer() {
		return customer;
	}

	public void setCustomer(String customer) {
		this.customer = customer;
	}

	

	public List<Customer> _getReviewerList() {
		return reviewerList;
	}

	public void _setReviewerList(List<Customer> reviewerList) {
		this.reviewerList = reviewerList;
	}
	public List<Product> _getReviewedProductList() {
		return reviewedProductList;
	}

	public void _setReviewedProductList(List<Product> reviewedProductList) {
		this.reviewedProductList = reviewedProductList;
	}
}
