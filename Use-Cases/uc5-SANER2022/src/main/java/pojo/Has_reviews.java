package pojo;

public class Has_reviews extends LoggingPojo {

	private Feedback reviews;	
	private Product reviewedProduct;	

	public Has_reviews() {}

	@Override
    public Object clone() throws CloneNotSupportedException {
        Has_reviews cloned = (Has_reviews) super.clone();
		cloned.setReviews((Feedback)cloned.getReviews().clone());	
		cloned.setReviewedProduct((Product)cloned.getReviewedProduct().clone());	
		return cloned;
    }

	public Feedback getReviews() {
		return reviews;
	}	

	public void setReviews(Feedback reviews) {
		this.reviews = reviews;
	}
	
	public Product getReviewedProduct() {
		return reviewedProduct;
	}	

	public void setReviewedProduct(Product reviewedProduct) {
		this.reviewedProduct = reviewedProduct;
	}
	

}
