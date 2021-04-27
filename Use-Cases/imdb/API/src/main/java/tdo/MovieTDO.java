package tdo;

import pojo.Movie;

public class MovieTDO extends Movie {
	private String myRelSchema_directed_movie_info_id;
	public String getMyRelSchema_directed_movie_info_id() {
		return this.myRelSchema_directed_movie_info_id;
	}

	public void setMyRelSchema_directed_movie_info_id( String myRelSchema_directed_movie_info_id) {
		this.myRelSchema_directed_movie_info_id = myRelSchema_directed_movie_info_id;
	}

	private String myRelSchema_directed_has_directed_id;
	public String getMyRelSchema_directed_has_directed_id() {
		return this.myRelSchema_directed_has_directed_id;
	}

	public void setMyRelSchema_directed_has_directed_id( String myRelSchema_directed_has_directed_id) {
		this.myRelSchema_directed_has_directed_id = myRelSchema_directed_has_directed_id;
	}

}
