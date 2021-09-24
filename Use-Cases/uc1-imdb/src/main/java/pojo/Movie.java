package pojo;

public class Movie extends LoggingPojo {

	private String id;
	private String primaryTitle;
	private String originalTitle;
	private Boolean isAdult;
	private Integer startYear;
	private Integer runtimeMinutes;
	private String averageRating;
	private Integer numVotes;

	public enum movieDirector {
		directed_movie
	}
	private java.util.List<Director> directorList;
	public enum movieActor {
		movie
	}
	private java.util.List<Actor> characterList;

	
	public Movie() {}

	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
	
	@Override
	public String toString(){
		return "Movie { " + "id="+id +", "+
"primaryTitle="+primaryTitle +", "+
"originalTitle="+originalTitle +", "+
"isAdult="+isAdult +", "+
"startYear="+startYear +", "+
"runtimeMinutes="+runtimeMinutes +", "+
"averageRating="+averageRating +", "+
"numVotes="+numVotes +"}"; 
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	public String getPrimaryTitle() {
		return primaryTitle;
	}

	public void setPrimaryTitle(String primaryTitle) {
		this.primaryTitle = primaryTitle;
	}
	public String getOriginalTitle() {
		return originalTitle;
	}

	public void setOriginalTitle(String originalTitle) {
		this.originalTitle = originalTitle;
	}
	public Boolean getIsAdult() {
		return isAdult;
	}

	public void setIsAdult(Boolean isAdult) {
		this.isAdult = isAdult;
	}
	public Integer getStartYear() {
		return startYear;
	}

	public void setStartYear(Integer startYear) {
		this.startYear = startYear;
	}
	public Integer getRuntimeMinutes() {
		return runtimeMinutes;
	}

	public void setRuntimeMinutes(Integer runtimeMinutes) {
		this.runtimeMinutes = runtimeMinutes;
	}
	public String getAverageRating() {
		return averageRating;
	}

	public void setAverageRating(String averageRating) {
		this.averageRating = averageRating;
	}
	public Integer getNumVotes() {
		return numVotes;
	}

	public void setNumVotes(Integer numVotes) {
		this.numVotes = numVotes;
	}

	

	public java.util.List<Director> _getDirectorList() {
		return directorList;
	}

	public void _setDirectorList(java.util.List<Director> directorList) {
		this.directorList = directorList;
	}
	public java.util.List<Actor> _getCharacterList() {
		return characterList;
	}

	public void _setCharacterList(java.util.List<Actor> characterList) {
		this.characterList = characterList;
	}
}
