package dao.impl;

import exceptions.PhysicalStructureException;
import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import conditions.Condition;
import java.util.HashSet;
import java.util.Set;
import conditions.AndCondition;
import conditions.OrCondition;
import conditions.SimpleCondition;
import conditions.MovieReviewAttribute;
import conditions.Operator;
import pojo.MovieReview;
import tdo.MovieTDO;
import tdo.MovieReviewTDO;
import pojo.Movie;
import conditions.MovieAttribute;
import tdo.ReviewTDO;
import tdo.MovieReviewTDO;
import pojo.Review;
import conditions.ReviewAttribute;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.commons.lang.mutable.MutableBoolean;
import util.Util;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Row;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import scala.collection.mutable.WrappedArray;
import org.apache.spark.api.java.function.FlatMapFunction;
import dbconnection.SparkConnectionMgr;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.ArrayType;
import static com.mongodb.client.model.Updates.addToSet;


public class MovieReviewServiceImpl extends dao.services.MovieReviewService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MovieReviewServiceImpl.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	
	
	// method accessing the embedded object movie mapped to role r_review
	public Dataset<pojo.MovieReview> getMovieReviewListInIMDB_MongoreviewColmovie(Condition<ReviewAttribute> r_review_condition, Condition<MovieAttribute> r_reviewed_movie_condition, MutableBoolean r_review_refilter, MutableBoolean r_reviewed_movie_refilter){	
			List<String> bsons = new ArrayList<String>();
			String bson = null;
			bson = MovieServiceImpl.getBSONMatchQueryInReviewColFromMymongo(r_reviewed_movie_condition ,r_reviewed_movie_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
			bson = ReviewServiceImpl.getBSONMatchQueryInReviewColFromMymongo(r_review_condition ,r_review_refilter);
			if(bson != null)
				bsons.add("{" + bson + "}");
		
			String bsonQuery = bsons.size() == 0 ? null : "{$match: { $and: [" + String.join(",", bsons) + "] }}";
		
			Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mymongo", "reviewCol", bsonQuery);
		
			Dataset<MovieReview> res = dataset.flatMap((FlatMapFunction<Row, MovieReview>) r -> {
					List<MovieReview> list_res = new ArrayList<MovieReview>();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					Row nestedRow = null;
		
					boolean addedInList = false;
					Row r1 = r;
					MovieReview movieReview1 = new MovieReview();
					movieReview1.setR_reviewed_movie(new Movie());
					movieReview1.setR_review(new Review());
					
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Review.id for field idreview			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("idreview")) {
						if(nestedRow.getAs("idreview")==null)
							movieReview1.getR_review().setId(null);
						else{
							movieReview1.getR_review().setId((String)nestedRow.getAs("idreview"));
							toAdd1 = true;					
							}
					}
					// 	attribute Review.content for field content			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("content")) {
						if(nestedRow.getAs("content")==null)
							movieReview1.getR_review().setContent(null);
						else{
							movieReview1.getR_review().setContent((String)nestedRow.getAs("content"));
							toAdd1 = true;					
							}
					}
					// 	attribute Review.note for field note			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("note")) {
						if(nestedRow.getAs("note")==null)
							movieReview1.getR_review().setNote(null);
						else{
							movieReview1.getR_review().setNote((Integer)nestedRow.getAs("note"));
							toAdd1 = true;					
							}
					}
					// 	attribute Movie.id for field movieid			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("movie");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("movieid")) {
						if(nestedRow.getAs("movieid")==null)
							movieReview1.getR_reviewed_movie().setId(null);
						else{
							movieReview1.getR_reviewed_movie().setId((String)nestedRow.getAs("movieid"));
							toAdd1 = true;					
							}
					}
					// 	attribute Movie.primaryTitle for field title			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("movie");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("title")) {
						if(nestedRow.getAs("title")==null)
							movieReview1.getR_reviewed_movie().setPrimaryTitle(null);
						else{
							movieReview1.getR_reviewed_movie().setPrimaryTitle((String)nestedRow.getAs("title"));
							toAdd1 = true;					
							}
					}
					// 	attribute Movie.averageRating for field avgrating			
					nestedRow =  r1;
					nestedRow = (nestedRow == null) ? null : (Row) nestedRow.getAs("movie");
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("avgrating")) {
						if(nestedRow.getAs("avgrating")==null)
							movieReview1.getR_reviewed_movie().setAverageRating(null);
						else{
							movieReview1.getR_reviewed_movie().setAverageRating((String)nestedRow.getAs("avgrating"));
							toAdd1 = true;					
							}
					}
					if(toAdd1) {
						
						list_res.add(movieReview1);
						addedInList = true;
					} 
					
					
					return list_res.iterator();
		
			}, Encoders.bean(MovieReview.class));
			// TODO drop duplicates based on roles ids
			//res= res.dropDuplicates(new String {});
			return res;
	}
	
	
	
	public java.util.List<pojo.MovieReview> getMovieReviewList(
		conditions.Condition<conditions.MovieAttribute> r_reviewed_movie_condition,
		conditions.Condition<conditions.ReviewAttribute> r_review_condition){
			//TODO
			return null;
		}
	
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
	
	public void insertMovieReview(MovieReview movieReview){
		//TODO
	}
	
	public void deleteMovieReviewList(
		conditions.Condition<conditions.MovieAttribute> r_reviewed_movie_condition,
		conditions.Condition<conditions.ReviewAttribute> r_review_condition){
			//TODO
		}
	
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
