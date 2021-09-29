package pojo;

public class User extends LoggingPojo {

	private String id;
	private String username;
	private String city;

	public enum reviewUser {
		r_author
	}
	private java.util.List<Review> r_review1List;

	
	public User() {}

	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
	
	@Override
	public String toString(){
		return "User { " + "id="+id +", "+
"username="+username +", "+
"city="+city +"}"; 
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}
	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	

	public java.util.List<Review> _getR_review1List() {
		return r_review1List;
	}

	public void _setR_review1List(java.util.List<Review> r_review1List) {
		this.r_review1List = r_review1List;
	}
}
