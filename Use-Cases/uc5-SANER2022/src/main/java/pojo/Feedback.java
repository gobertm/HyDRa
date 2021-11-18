package pojo;

public class Feedback extends LoggingPojo {

	private Product reviewedProduct;	
	private Customer reviewer;	
	private Double rate;	
	private String content;	

	public Feedback() {}

	@Override
    public Object clone() throws CloneNotSupportedException {
        Feedback cloned = (Feedback) super.clone();
		cloned.setReviewedProduct((Product)cloned.getReviewedProduct().clone());	
		cloned.setReviewer((Customer)cloned.getReviewer().clone());	
		return cloned;
    }

	public Product getReviewedProduct() {
		return reviewedProduct;
	}	

	public void setReviewedProduct(Product reviewedProduct) {
		this.reviewedProduct = reviewedProduct;
	}
	
	public Customer getReviewer() {
		return reviewer;
	}	

	public void setReviewer(Customer reviewer) {
		this.reviewer = reviewer;
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
	

}
