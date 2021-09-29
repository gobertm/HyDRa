package pojo;

public class Review extends LoggingPojo {

	private String id;
	private String content;
	private Integer note;

	public enum movieReview {
		r_review
	}
	private Movie r_reviewed_movie;
	public enum reviewUser {
		r_review1
	}
	private User r_author;

	
	public Review() {}

	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
	
	@Override
	public String toString(){
		return "Review { " + "id="+id +", "+
"content="+content +", "+
"note="+note +"}"; 
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}
	public Integer getNote() {
		return note;
	}

	public void setNote(Integer note) {
		this.note = note;
	}

	

	public Movie _getR_reviewed_movie() {
		return r_reviewed_movie;
	}

	public void _setR_reviewed_movie(Movie r_reviewed_movie) {
		this.r_reviewed_movie = r_reviewed_movie;
	}
	public User _getR_author() {
		return r_author;
	}

	public void _setR_author(User r_author) {
		this.r_author = r_author;
	}
}
