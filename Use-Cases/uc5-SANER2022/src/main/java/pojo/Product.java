package pojo;
import java.time.LocalDate;
import java.util.List;

public class Product extends LoggingPojo {

	private String id;
	private String title;
	private Double price;
	private String photo;

	private List<Feedback> feedbackListAsReviewedProduct;
	public enum composed_of {
		orderedProducts
	}
	private List<Order> orderPList;

	// Empty constructor
	public Product() {}

	/*
	* Constructor on simple attribute 
	*/
	public Product(String id,String title,Double price,String photo) {
		this.id = id;
		this.title = title;
		this.price = price;
		this.photo = photo;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
	
	@Override
	public String toString(){
		return "Product { " + "id="+id +", "+
					"title="+title +", "+
					"price="+price +", "+
					"photo="+photo +"}"; 
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}
	public Double getPrice() {
		return price;
	}

	public void setPrice(Double price) {
		this.price = price;
	}
	public String getPhoto() {
		return photo;
	}

	public void setPhoto(String photo) {
		this.photo = photo;
	}

	

	public java.util.List<Feedback> _getFeedbackListAsReviewedProduct() {
		return feedbackListAsReviewedProduct;
	}

	public void _setFeedbackListAsReviewedProduct(java.util.List<Feedback> feedbackListAsReviewedProduct) {
		this.feedbackListAsReviewedProduct = feedbackListAsReviewedProduct;
	}
	public List<Order> _getOrderPList() {
		return orderPList;
	}

	public void _setOrderPList(List<Order> orderPList) {
		this.orderPList = orderPList;
	}
}
