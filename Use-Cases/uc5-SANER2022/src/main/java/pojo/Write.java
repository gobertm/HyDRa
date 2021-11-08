package pojo;

public class Write extends LoggingPojo {

	private Feedback review;	
	private Customer reviewer;	

	public Write() {}

	@Override
    public Object clone() throws CloneNotSupportedException {
        Write cloned = (Write) super.clone();
		cloned.setReview((Feedback)cloned.getReview().clone());	
		cloned.setReviewer((Customer)cloned.getReviewer().clone());	
		return cloned;
    }

	public Feedback getReview() {
		return review;
	}	

	public void setReview(Feedback review) {
		this.review = review;
	}
	
	public Customer getReviewer() {
		return reviewer;
	}	

	public void setReviewer(Customer reviewer) {
		this.reviewer = reviewer;
	}
	

}
