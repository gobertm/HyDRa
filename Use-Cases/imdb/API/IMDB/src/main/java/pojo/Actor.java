package pojo;

public class Actor extends LoggingPojo {

	private String id;
	private String fullName;
	private String yearOfBirth;
	private String yearOfDeath;

	public enum movieActor {
		character
	}
	private java.util.List<Movie> movieList;

	
	public Actor() {}

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
	public String getFullName() {
		return fullName;
	}

	public void setFullName(String fullName) {
		this.fullName = fullName;
	}
	public String getYearOfBirth() {
		return yearOfBirth;
	}

	public void setYearOfBirth(String yearOfBirth) {
		this.yearOfBirth = yearOfBirth;
	}
	public String getYearOfDeath() {
		return yearOfDeath;
	}

	public void setYearOfDeath(String yearOfDeath) {
		this.yearOfDeath = yearOfDeath;
	}

	

	public java.util.List<Movie> _getMovieList() {
		return movieList;
	}

	public void _setMovieList(java.util.List<Movie> movieList) {
		this.movieList = movieList;
	}
}
