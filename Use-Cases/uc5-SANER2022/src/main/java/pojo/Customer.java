package pojo;
import java.time.LocalDate;
import java.util.List;

public class Customer extends LoggingPojo {

	private String id;
	private String firstname;
	private String lastname;
	private String gender;
	private LocalDate birthday;
	private LocalDate creationDate;
	private String locationip;
	private String browser;

	public enum buys {
		client
	}
	private List<Order> orderList;
	public enum write {
		reviewer
	}
	private List<Feedback> reviewList;

	// Empty constructor
	public Customer() {}

	/*
	* Constructor on simple attribute 
	*/
	public Customer(String id,String firstname,String lastname,String gender,LocalDate birthday,LocalDate creationDate,String locationip,String browser) {
		this.id = id;
		this.firstname = firstname;
		this.lastname = lastname;
		this.gender = gender;
		this.birthday = birthday;
		this.creationDate = creationDate;
		this.locationip = locationip;
		this.browser = browser;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
	
	@Override
	public String toString(){
		return "Customer { " + "id="+id +", "+
					"firstname="+firstname +", "+
					"lastname="+lastname +", "+
					"gender="+gender +", "+
					"birthday="+birthday +", "+
					"creationDate="+creationDate +", "+
					"locationip="+locationip +", "+
					"browser="+browser +"}"; 
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	public String getFirstname() {
		return firstname;
	}

	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}
	public String getLastname() {
		return lastname;
	}

	public void setLastname(String lastname) {
		this.lastname = lastname;
	}
	public String getGender() {
		return gender;
	}

	public void setGender(String gender) {
		this.gender = gender;
	}
	public LocalDate getBirthday() {
		return birthday;
	}

	public void setBirthday(LocalDate birthday) {
		this.birthday = birthday;
	}
	public LocalDate getCreationDate() {
		return creationDate;
	}

	public void setCreationDate(LocalDate creationDate) {
		this.creationDate = creationDate;
	}
	public String getLocationip() {
		return locationip;
	}

	public void setLocationip(String locationip) {
		this.locationip = locationip;
	}
	public String getBrowser() {
		return browser;
	}

	public void setBrowser(String browser) {
		this.browser = browser;
	}

	

	public List<Order> _getOrderList() {
		return orderList;
	}

	public void _setOrderList(List<Order> orderList) {
		this.orderList = orderList;
	}
	public List<Feedback> _getReviewList() {
		return reviewList;
	}

	public void _setReviewList(List<Feedback> reviewList) {
		this.reviewList = reviewList;
	}
}
