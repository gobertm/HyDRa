package pojo;

public class MovieDirector extends LoggingPojo {

	private Movie directed_movie;	
	private Director director;	

	public MovieDirector() {}

	@Override
    public Object clone() throws CloneNotSupportedException {
        MovieDirector cloned = (MovieDirector) super.clone();
		cloned.setDirected_movie((Movie)cloned.getDirected_movie().clone());	
		cloned.setDirector((Director)cloned.getDirector().clone());	
		return cloned;
    }

	public Movie getDirected_movie() {
		return directed_movie;
	}	

	public void setDirected_movie(Movie directed_movie) {
		this.directed_movie = directed_movie;
	}
	
	public Director getDirector() {
		return director;
	}	

	public void setDirector(Director director) {
		this.director = director;
	}
	

}
