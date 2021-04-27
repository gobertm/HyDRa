package pojo;

public class Director extends LoggingPojo {

	private String id;
	private String firstName;
	private String lastName;
	private Integer yearOfBirth;
	private Integer yearOfDeath;

	public enum movieDirector {
		director
	}
	private java.util.List<Movie> directed_movieList;

	
	public Director() {}

	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
	

	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	public Integer getYearOfBirth() {
		return yearOfBirth;
	}

	public void setYearOfBirth(Integer yearOfBirth) {
		this.yearOfBirth = yearOfBirth;
	}
	public Integer getYearOfDeath() {
		return yearOfDeath;
	}

	public void setYearOfDeath(Integer yearOfDeath) {
		this.yearOfDeath = yearOfDeath;
	}

	

	public java.util.List<Movie> _getDirected_movieList() {
		return directed_movieList;
	}

	public void _setDirected_movieList(java.util.List<Movie> directed_movieList) {
		this.directed_movieList = directed_movieList;
	}
}
