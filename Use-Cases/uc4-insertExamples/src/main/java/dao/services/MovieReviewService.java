package dao.services;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import conditions.Condition;
import pojo.MovieReview;
import tdo.MovieTDO;
import tdo.MovieReviewTDO;
import pojo.Movie;
import pojo.MovieReview;
import conditions.MovieAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;
import tdo.ReviewTDO;
import tdo.MovieReviewTDO;
import pojo.Review;
import pojo.MovieReview;
import conditions.ReviewAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;


public abstract class MovieReviewService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MovieReviewService.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	
	
	// method accessing the embedded object movie mapped to role r_review
	public abstract Dataset<pojo.MovieReview> getMovieReviewListInIMDB_MongoreviewColmovie(Condition<ReviewAttribute> r_review_condition, Condition<MovieAttribute> r_reviewed_movie_condition, MutableBoolean r_review_refilter, MutableBoolean r_reviewed_movie_refilter);
	
	
	
	public abstract java.util.List<pojo.MovieReview> getMovieReviewList(
		conditions.Condition<conditions.MovieAttribute> r_reviewed_movie_condition,
		conditions.Condition<conditions.ReviewAttribute> r_review_condition);
	
	public java.util.List<pojo.MovieReview> getMovieReviewListByR_reviewed_movieCondition(
		conditions.Condition<conditions.MovieAttribute> r_reviewed_movie_condition
	){
		return getMovieReviewList(r_reviewed_movie_condition, null);
	}
	
	public java.util.List<pojo.MovieReview> getMovieReviewListByR_reviewed_movie(pojo.Movie r_reviewed_movie) {
		// TODO using id for selecting
		return null;
	}
	public java.util.List<pojo.MovieReview> getMovieReviewListByR_reviewCondition(
		conditions.Condition<conditions.ReviewAttribute> r_review_condition
	){
		return getMovieReviewList(null, r_review_condition);
	}
	
	public pojo.MovieReview getMovieReviewByR_review(pojo.Review r_review) {
		// TODO using id for selecting
		return null;
	}
	
	public abstract void insertMovieReview(MovieReview movieReview);
	
	public abstract void deleteMovieReviewList(
		conditions.Condition<conditions.MovieAttribute> r_reviewed_movie_condition,
		conditions.Condition<conditions.ReviewAttribute> r_review_condition);
	
	public void deleteMovieReviewListByR_reviewed_movieCondition(
		conditions.Condition<conditions.MovieAttribute> r_reviewed_movie_condition
	){
		deleteMovieReviewList(r_reviewed_movie_condition, null);
	}
	
	public void deleteMovieReviewListByR_reviewed_movie(pojo.Movie r_reviewed_movie) {
		// TODO using id for selecting
		return;
	}
	public void deleteMovieReviewListByR_reviewCondition(
		conditions.Condition<conditions.ReviewAttribute> r_review_condition
	){
		deleteMovieReviewList(null, r_review_condition);
	}
	
	public void deleteMovieReviewByR_review(pojo.Review r_review) {
		// TODO using id for selecting
		return;
	}
		
}
