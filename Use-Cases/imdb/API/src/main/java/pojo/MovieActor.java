package pojo;

public class MovieActor extends LoggingPojo {

	private Actor character;	
	private Movie movie;	

	public MovieActor() {}

	@Override
    public Object clone() throws CloneNotSupportedException {
        MovieActor cloned = (MovieActor) super.clone();
		cloned.setCharacter((Actor)cloned.getCharacter().clone());	
		cloned.setMovie((Movie)cloned.getMovie().clone());	
		return cloned;
    }

	public Actor getCharacter() {
		return character;
	}	

	public void setCharacter(Actor character) {
		this.character = character;
	}
	
	public Movie getMovie() {
		return movie;
	}	

	public void setMovie(Movie movie) {
		this.movie = movie;
	}
	

}
