package pojo;

public class ReviewUser extends LoggingPojo {

	private User r_author;	
	private Review r_review1;	

	public ReviewUser() {}

	@Override
    public Object clone() throws CloneNotSupportedException {
        ReviewUser cloned = (ReviewUser) super.clone();
		cloned.setR_author((User)cloned.getR_author().clone());	
		cloned.setR_review1((Review)cloned.getR_review1().clone());	
		return cloned;
    }

	public User getR_author() {
		return r_author;
	}	

	public void setR_author(User r_author) {
		this.r_author = r_author;
	}
	
	public Review getR_review1() {
		return r_review1;
	}	

	public void setR_review1(Review r_review1) {
		this.r_review1 = r_review1;
	}
	

}
