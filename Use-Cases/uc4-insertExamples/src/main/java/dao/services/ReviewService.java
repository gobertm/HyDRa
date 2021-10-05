package dao.services;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pojo.Review;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.commons.lang.mutable.MutableBoolean;
import conditions.Condition;
import conditions.Operator;
import util.Util;
import conditions.ReviewAttribute;
import conditions.MovieAttribute;
import pojo.Movie;
import conditions.ReviewAttribute;
import conditions.UserAttribute;
import pojo.User;

public abstract class ReviewService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReviewService.class);
	protected MovieReviewService movieReviewService = new dao.impl.MovieReviewServiceImpl();
	protected ReviewUserService reviewUserService = new dao.impl.ReviewUserServiceImpl();
	


	public static enum ROLE_NAME {
		MOVIEREVIEW_R_REVIEW, REVIEWUSER_R_REVIEW1
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.MOVIEREVIEW_R_REVIEW, loading.Loading.EAGER);
		defaultLoadingParameters.put(ROLE_NAME.REVIEWUSER_R_REVIEW1, loading.Loading.EAGER);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public ReviewService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public ReviewService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
		this();
		if(loadingParams != null)
			for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: loadingParams.entrySet())
				loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public static java.util.Map<ROLE_NAME, loading.Loading> getDefaultLoadingParameters() {
		java.util.Map<ROLE_NAME, loading.Loading> res = new java.util.HashMap<ROLE_NAME, loading.Loading>();
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
				res.put(entry.getKey(), entry.getValue());
		return res;
	}
	
	public static void setAllDefaultLoadingParameters(loading.Loading loading) {
		java.util.Map<ROLE_NAME, loading.Loading> newParams = new java.util.HashMap<ROLE_NAME, loading.Loading>();
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
				newParams.put(entry.getKey(), entry.getValue());
		defaultLoadingParameters = newParams;
	}
	
	public java.util.Map<ROLE_NAME, loading.Loading> getLoadingParameters() {
		return this.loadingParameters;
	}
	
	public void setLoadingParameters(java.util.Map<ROLE_NAME, loading.Loading> newParams) {
		this.loadingParameters = newParams;
	}
	
	public void updateLoadingParameter(ROLE_NAME role, loading.Loading l) {
		this.loadingParameters.put(role, l);
	}
	
	
	public Dataset<Review> getReviewList(conditions.Condition<conditions.ReviewAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Review>> datasets = new ArrayList<Dataset<Review>>();
		Dataset<Review> d = null;
		d = getReviewListInReviewTableFromMydb(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		d = getReviewListInReviewColFromMymongo(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsReview(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Review>) r -> condition == null || condition.evaluate(r));
		d=d.dropDuplicates();
		return d;
	}
	
	
	
	
	public abstract Dataset<Review> getReviewListInReviewTableFromMydb(conditions.Condition<conditions.ReviewAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	public abstract Dataset<Review> getReviewListInReviewColFromMymongo(conditions.Condition<conditions.ReviewAttribute> condition, MutableBoolean refilterFlag);
	
	
	// TODO get based on id(s). Ex:public Client getClientById(Long id)
	
	public Dataset<Review> getReviewListById(String id) {
		return getReviewList(conditions.Condition.simple(conditions.ReviewAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Review> getReviewListByContent(String content) {
		return getReviewList(conditions.Condition.simple(conditions.ReviewAttribute.content, conditions.Operator.EQUALS, content));
	}
	
	public Dataset<Review> getReviewListByNote(Integer note) {
		return getReviewList(conditions.Condition.simple(conditions.ReviewAttribute.note, conditions.Operator.EQUALS, note));
	}
	
	
	
	protected static Dataset<Review> fullOuterJoinsReview(List<Dataset<Review>> datasetsPOJO) {
		return fullOuterJoinsReview(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Review> fullLeftOuterJoinsReview(List<Dataset<Review>> datasetsPOJO) {
		return fullOuterJoinsReview(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Review> fullOuterJoinsReview(List<Dataset<Review>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Review> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("content", "content_1")
								.withColumnRenamed("note", "note_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("content", "content_" + i)
								.withColumnRenamed("note", "note_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			} 
			d = res.map((MapFunction<Row, Review>) r -> {
					Review review_res = new Review();
					
					// attribute 'Review.id'
					String firstNotNull_id = Util.getStringValue(r.getAs("id"));
					review_res.setId(firstNotNull_id);
					
					// attribute 'Review.content'
					String firstNotNull_content = Util.getStringValue(r.getAs("content"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String content2 = Util.getStringValue(r.getAs("content_" + i));
						if (firstNotNull_content != null && content2 != null && !firstNotNull_content.equals(content2)) {
							review_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Review.content': " + firstNotNull_content + " and " + content2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Review.content' ==> " + firstNotNull_content + " and " + content2);
						}
						if (firstNotNull_content == null && content2 != null) {
							firstNotNull_content = content2;
						}
					}
					review_res.setContent(firstNotNull_content);
					
					// attribute 'Review.note'
					Integer firstNotNull_note = Util.getIntegerValue(r.getAs("note"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer note2 = Util.getIntegerValue(r.getAs("note_" + i));
						if (firstNotNull_note != null && note2 != null && !firstNotNull_note.equals(note2)) {
							review_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Review.note': " + firstNotNull_note + " and " + note2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Review.note' ==> " + firstNotNull_note + " and " + note2);
						}
						if (firstNotNull_note == null && note2 != null) {
							firstNotNull_note = note2;
						}
					}
					review_res.setNote(firstNotNull_note);
	
					scala.collection.mutable.WrappedArray<String> logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							review_res.addLogEvent(logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							review_res.addLogEvent(logEvents.apply(j));
						}
					}
	
					return review_res;
				}, Encoders.bean(Review.class));
			return d;
	}
	
	
	
	
	public Dataset<Review> getReviewList(Review.movieReview role, Movie movie) {
		if(role != null) {
			if(role.equals(Review.movieReview.r_review))
				return getR_reviewListInMovieReviewByR_reviewed_movie(movie);
		}
		return null;
	}
	
	public Dataset<Review> getReviewList(Review.movieReview role, Condition<MovieAttribute> condition) {
		if(role != null) {
			if(role.equals(Review.movieReview.r_review))
				return getR_reviewListInMovieReviewByR_reviewed_movieCondition(condition);
		}
		return null;
	}
	
	public Dataset<Review> getReviewList(Review.movieReview role, Condition<MovieAttribute> condition1, Condition<ReviewAttribute> condition2) {
		if(role != null) {
			if(role.equals(Review.movieReview.r_review))
				return getR_reviewListInMovieReview(condition1, condition2);
		}
		return null;
	}
	
	
	
	public Dataset<Review> getReviewList(Review.reviewUser role, User user) {
		if(role != null) {
			if(role.equals(Review.reviewUser.r_review1))
				return getR_review1ListInReviewUserByR_author(user);
		}
		return null;
	}
	
	public Dataset<Review> getReviewList(Review.reviewUser role, Condition<UserAttribute> condition) {
		if(role != null) {
			if(role.equals(Review.reviewUser.r_review1))
				return getR_review1ListInReviewUserByR_authorCondition(condition);
		}
		return null;
	}
	
	public Dataset<Review> getReviewList(Review.reviewUser role, Condition<UserAttribute> condition1, Condition<ReviewAttribute> condition2) {
		if(role != null) {
			if(role.equals(Review.reviewUser.r_review1))
				return getR_review1ListInReviewUser(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	public abstract Dataset<Review> getR_reviewListInMovieReview(conditions.Condition<conditions.MovieAttribute> r_reviewed_movie_condition,conditions.Condition<conditions.ReviewAttribute> r_review_condition);
	
	public Dataset<Review> getR_reviewListInMovieReviewByR_reviewed_movieCondition(conditions.Condition<conditions.MovieAttribute> r_reviewed_movie_condition){
		return getR_reviewListInMovieReview(r_reviewed_movie_condition, null);
	}
	
	public Dataset<Review> getR_reviewListInMovieReviewByR_reviewed_movie(pojo.Movie r_reviewed_movie){
		if(r_reviewed_movie == null)
			return null;
	
		Condition c;
		c=Condition.simple(MovieAttribute.id,Operator.EQUALS, r_reviewed_movie.getId());
		Dataset<Review> res = getR_reviewListInMovieReviewByR_reviewed_movieCondition(c);
		return res;
	}
	
	public Dataset<Review> getR_reviewListInMovieReviewByR_reviewCondition(conditions.Condition<conditions.ReviewAttribute> r_review_condition){
		return getR_reviewListInMovieReview(null, r_review_condition);
	}
	public abstract Dataset<Review> getR_review1ListInReviewUser(conditions.Condition<conditions.UserAttribute> r_author_condition,conditions.Condition<conditions.ReviewAttribute> r_review1_condition);
	
	public Dataset<Review> getR_review1ListInReviewUserByR_authorCondition(conditions.Condition<conditions.UserAttribute> r_author_condition){
		return getR_review1ListInReviewUser(r_author_condition, null);
	}
	
	public Dataset<Review> getR_review1ListInReviewUserByR_author(pojo.User r_author){
		if(r_author == null)
			return null;
	
		Condition c;
		c=Condition.simple(UserAttribute.id,Operator.EQUALS, r_author.getId());
		Dataset<Review> res = getR_review1ListInReviewUserByR_authorCondition(c);
		return res;
	}
	
	public Dataset<Review> getR_review1ListInReviewUserByR_review1Condition(conditions.Condition<conditions.ReviewAttribute> r_review1_condition){
		return getR_review1ListInReviewUser(null, r_review1_condition);
	}
	
		/**
		Review is mapped to complex embedded physical structures : [reviewCol]
		Please make sure parameters have their mandatory role objects set (not null).
	,
		 @param r_reviewed_movieMovieReview
	,
		 @param r_authorReviewUser	*/
	public abstract boolean insertReview(
		Review review,
		Movie	r_reviewed_movieMovieReview,
		User	r_authorReviewUser);
	
	public abstract boolean insertReviewInReviewTableFromMydb(Review review); 
	public abstract boolean insertReviewInReviewColFromMymongo(Review review,
		Movie	r_reviewed_movieMovieReview,
		User	r_authorReviewUser);
	public abstract void updateReviewList(conditions.Condition<conditions.ReviewAttribute> condition, conditions.SetClause<conditions.ReviewAttribute> set);
	
	public void updateReview(pojo.Review review) {
		//TODO using the id
		return;
	}
	public abstract void updateR_reviewListInMovieReview(
		conditions.Condition<conditions.MovieAttribute> r_reviewed_movie_condition,
		conditions.Condition<conditions.ReviewAttribute> r_review_condition,
		
		conditions.SetClause<conditions.ReviewAttribute> set
	);
	
	public void updateR_reviewListInMovieReviewByR_reviewed_movieCondition(
		conditions.Condition<conditions.MovieAttribute> r_reviewed_movie_condition,
		conditions.SetClause<conditions.ReviewAttribute> set
	){
		updateR_reviewListInMovieReview(r_reviewed_movie_condition, null, set);
	}
	
	public void updateR_reviewListInMovieReviewByR_reviewed_movie(
		pojo.Movie r_reviewed_movie,
		conditions.SetClause<conditions.ReviewAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateR_reviewListInMovieReviewByR_reviewCondition(
		conditions.Condition<conditions.ReviewAttribute> r_review_condition,
		conditions.SetClause<conditions.ReviewAttribute> set
	){
		updateR_reviewListInMovieReview(null, r_review_condition, set);
	}
	public abstract void updateR_review1ListInReviewUser(
		conditions.Condition<conditions.UserAttribute> r_author_condition,
		conditions.Condition<conditions.ReviewAttribute> r_review1_condition,
		
		conditions.SetClause<conditions.ReviewAttribute> set
	);
	
	public void updateR_review1ListInReviewUserByR_authorCondition(
		conditions.Condition<conditions.UserAttribute> r_author_condition,
		conditions.SetClause<conditions.ReviewAttribute> set
	){
		updateR_review1ListInReviewUser(r_author_condition, null, set);
	}
	
	public void updateR_review1ListInReviewUserByR_author(
		pojo.User r_author,
		conditions.SetClause<conditions.ReviewAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateR_review1ListInReviewUserByR_review1Condition(
		conditions.Condition<conditions.ReviewAttribute> r_review1_condition,
		conditions.SetClause<conditions.ReviewAttribute> set
	){
		updateR_review1ListInReviewUser(null, r_review1_condition, set);
	}
	
	
	public abstract void deleteReviewList(conditions.Condition<conditions.ReviewAttribute> condition);
	
	public void deleteReview(pojo.Review review) {
		//TODO using the id
		return;
	}
	public abstract void deleteR_reviewListInMovieReview(	
		conditions.Condition<conditions.MovieAttribute> r_reviewed_movie_condition,	
		conditions.Condition<conditions.ReviewAttribute> r_review_condition);
	
	public void deleteR_reviewListInMovieReviewByR_reviewed_movieCondition(
		conditions.Condition<conditions.MovieAttribute> r_reviewed_movie_condition
	){
		deleteR_reviewListInMovieReview(r_reviewed_movie_condition, null);
	}
	
	public void deleteR_reviewListInMovieReviewByR_reviewed_movie(
		pojo.Movie r_reviewed_movie 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteR_reviewListInMovieReviewByR_reviewCondition(
		conditions.Condition<conditions.ReviewAttribute> r_review_condition
	){
		deleteR_reviewListInMovieReview(null, r_review_condition);
	}
	public abstract void deleteR_review1ListInReviewUser(	
		conditions.Condition<conditions.UserAttribute> r_author_condition,	
		conditions.Condition<conditions.ReviewAttribute> r_review1_condition);
	
	public void deleteR_review1ListInReviewUserByR_authorCondition(
		conditions.Condition<conditions.UserAttribute> r_author_condition
	){
		deleteR_review1ListInReviewUser(r_author_condition, null);
	}
	
	public void deleteR_review1ListInReviewUserByR_author(
		pojo.User r_author 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteR_review1ListInReviewUserByR_review1Condition(
		conditions.Condition<conditions.ReviewAttribute> r_review1_condition
	){
		deleteR_review1ListInReviewUser(null, r_review1_condition);
	}
	
}
