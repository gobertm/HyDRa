package pojo;

public class MovieReview extends LoggingPojo {

	private Movie r_reviewed_movie;	
	private Review r_review;	

	public MovieReview() {}

	@Override
    public Object clone() throws CloneNotSupportedException {
        MovieReview cloned = (MovieReview) super.clone();
		cloned.setR_reviewed_movie((Movie)cloned.getR_reviewed_movie().clone());	
		cloned.setR_review((Review)cloned.getR_review().clone());	
		return cloned;
    }

	public Movie getR_reviewed_movie() {
		return r_reviewed_movie;
	}	

	public void setR_reviewed_movie(Movie r_reviewed_movie) {
		this.r_reviewed_movie = r_reviewed_movie;
	}
	
	public Review getR_review() {
		return r_review;
	}	

	public void setR_review(Review r_review) {
		this.r_review = r_review;
	}
	

}
