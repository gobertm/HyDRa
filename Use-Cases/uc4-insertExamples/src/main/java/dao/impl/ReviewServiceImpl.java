package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Review;
import conditions.*;
import dao.services.ReviewService;
import util.Util;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.api.java.function.MapFunction;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import com.mongodb.spark.MongoSpark;
import org.bson.Document;
import static java.util.Collections.singletonList;
import dbconnection.SparkConnectionMgr;
import dbconnection.DBConnectionMgr;
import scala.collection.mutable.WrappedArray;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FilterFunction;
import java.util.ArrayList;
import org.apache.commons.lang.mutable.MutableBoolean;
import tdo.*;
import pojo.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.ArrayType;
import scala.Tuple2;
import org.bson.Document;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;


public class ReviewServiceImpl extends ReviewService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReviewServiceImpl.class);
	
	
	
	
	public static String getBSONMatchQueryInReviewColFromMymongo(Condition<ReviewAttribute> condition, MutableBoolean refilterFlag) {	
		String res = null;	
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				ReviewAttribute attr = ((SimpleCondition<ReviewAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<ReviewAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<ReviewAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == ReviewAttribute.id ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "idreview': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == ReviewAttribute.content ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "content': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(attr == ReviewAttribute.note ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "note': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						res = "$expr: {$eq:[1,1]}";
					}
					
				}
			}
	
			if(condition instanceof AndCondition) {
				String bsonLeft = getBSONMatchQueryInReviewColFromMymongo(((AndCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInReviewColFromMymongo(((AndCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $and: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";
			}
	
			if(condition instanceof OrCondition) {
				String bsonLeft = getBSONMatchQueryInReviewColFromMymongo(((OrCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInReviewColFromMymongo(((OrCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $or: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";	
			}
	
			
	
			
		}
	
		return res;
	}
	
	public Dataset<Review> getReviewListInReviewColFromMymongo(conditions.Condition<conditions.ReviewAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = ReviewServiceImpl.getBSONMatchQueryInReviewColFromMymongo(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mymongo", "reviewCol", bsonQuery);
	
		Dataset<Review> res = dataset.flatMap((FlatMapFunction<Row, Review>) r -> {
				List<Review> list_res = new ArrayList<Review>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				Review review1 = new Review();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Review.id for field idreview			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("idreview")) {
						if(nestedRow.getAs("idreview")==null)
							review1.setId(null);
						else{
							review1.setId((String) nestedRow.getAs("idreview"));
							toAdd1 = true;					
							}
					}
					// 	attribute Review.content for field content			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("content")) {
						if(nestedRow.getAs("content")==null)
							review1.setContent(null);
						else{
							review1.setContent((String) nestedRow.getAs("content"));
							toAdd1 = true;					
							}
					}
					// 	attribute Review.note for field note			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("note")) {
						if(nestedRow.getAs("note")==null)
							review1.setNote(null);
						else{
							review1.setNote((Integer) nestedRow.getAs("note"));
							toAdd1 = true;					
							}
					}
					if(toAdd1) {
						
						list_res.add(review1);
						addedInList = true;
					} 
					
				
				return list_res.iterator();
	
		}, Encoders.bean(Review.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
		
	}
	
	public static Pair<String, List<String>> getSQLWhereClauseInReviewTableFromMydb(Condition<ReviewAttribute> condition, MutableBoolean refilterFlag) {
		return getSQLWhereClauseInReviewTableFromMydbWithTableAlias(condition, refilterFlag, "");
	}
	
	public static Pair<String, List<String>> getSQLWhereClauseInReviewTableFromMydbWithTableAlias(Condition<ReviewAttribute> condition, MutableBoolean refilterFlag, String tableAlias) {
		String where = null;	
		List<String> preparedValues = new java.util.ArrayList<String>();
		if(condition != null) {
			
			if(condition instanceof SimpleCondition) {
				ReviewAttribute attr = ((SimpleCondition<ReviewAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<ReviewAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<ReviewAttribute>) condition).getValue();
				if(value != null) {
					boolean isConditionAttrEncountered = false;
					if(attr == ReviewAttribute.id ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "id " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(attr == ReviewAttribute.content ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "content " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						where = "1 = 1";
					}
				}
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInReviewTableFromMydb(((AndCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInReviewTableFromMydb(((AndCondition) condition).getRightCondition(), refilterFlag);
				String whereLeft = pairLeft.getKey();
				String whereRight = pairRight.getKey();
				List<String> leftValues = pairLeft.getValue();
				List<String> rightValues = pairRight.getValue();
				if(whereLeft != null || whereRight != null) {
					if(whereLeft == null)
						where = whereRight;
					else
						if(whereRight == null)
							where = whereLeft;
						else
							where = "(" + whereLeft + " AND " + whereRight + ")";
					preparedValues.addAll(leftValues);
					preparedValues.addAll(rightValues);
				}
			}
	
			if(condition instanceof OrCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInReviewTableFromMydb(((OrCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInReviewTableFromMydb(((OrCondition) condition).getRightCondition(), refilterFlag);
				String whereLeft = pairLeft.getKey();
				String whereRight = pairRight.getKey();
				List<String> leftValues = pairLeft.getValue();
				List<String> rightValues = pairRight.getValue();
				if(whereLeft != null || whereRight != null) {
					if(whereLeft == null)
						where = whereRight;
					else
						if(whereRight == null)
							where = whereLeft;
						else
							where = "(" + whereLeft + " OR " + whereRight + ")";
					preparedValues.addAll(leftValues);
					preparedValues.addAll(rightValues);
				}
			}
	
		}
	
		return new ImmutablePair<String, List<String>>(where, preparedValues);
	}
	
	
	public Dataset<Review> getReviewListInReviewTableFromMydb(conditions.Condition<conditions.ReviewAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = ReviewServiceImpl.getSQLWhereClauseInReviewTableFromMydb(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", "'" + Util.escapeQuote(preparedValue) + "'");
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("mydb", "reviewTable");
		if(where != null) {
			d = d.where(where);
		}
	
		Dataset<Review> res = d.map((MapFunction<Row, Review>) r -> {
					Review review_res = new Review();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Review.Id]
					String id = Util.getStringValue(r.getAs("id"));
					review_res.setId(id);
					
					// attribute [Review.Content]
					String content = Util.getStringValue(r.getAs("content"));
					review_res.setContent(content);
	
	
	
					return review_res;
				}, Encoders.bean(Review.class));
	
	
		return res;
		
	}
	
	
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
	
	
	
	
	public Dataset<Review> getR_reviewListInMovieReview(conditions.Condition<conditions.MovieAttribute> r_reviewed_movie_condition,conditions.Condition<conditions.ReviewAttribute> r_review_condition)		{
		MutableBoolean r_review_refilter = new MutableBoolean(false);
		List<Dataset<Review>> datasetsPOJO = new ArrayList<Dataset<Review>>();
		Dataset<Movie> all = new MovieServiceImpl().getMovieList(r_reviewed_movie_condition);
		boolean all_already_persisted = false;
		MutableBoolean r_reviewed_movie_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		
		Dataset<MovieReview> res_movieReview_r_review;
		Dataset<Review> res_Review;
		// Role 'r_review' mapped to EmbeddedObject 'movie' - 'Movie' containing 'Review'
		r_reviewed_movie_refilter = new MutableBoolean(false);
		res_movieReview_r_review = movieReviewService.getMovieReviewListInIMDB_MongoreviewColmovie(r_review_condition, r_reviewed_movie_condition, r_review_refilter, r_reviewed_movie_refilter);
		if(r_reviewed_movie_refilter.booleanValue()) {
			joinCondition = null;
			joinCondition = res_movieReview_r_review.col("r_reviewed_movie.id").equalTo(all.col("id"));
			if(joinCondition == null)
				res_Review = res_movieReview_r_review.join(all).select("r_review.*").as(Encoders.bean(Review.class));
			else
				res_Review = res_movieReview_r_review.join(all, joinCondition).select("r_review.*").as(Encoders.bean(Review.class));
		
		} else
			res_Review = res_movieReview_r_review.map((MapFunction<MovieReview,Review>) r -> r.getR_review(), Encoders.bean(Review.class));
		res_Review = res_Review.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Review);
		
		
		//Join datasets or return 
		Dataset<Review> res = fullOuterJoinsReview(datasetsPOJO);
		if(res == null)
			return null;
	
		List<Dataset<Review>> lonelyReviewList = new ArrayList<Dataset<Review>>();
		lonelyReviewList.add(getReviewListInReviewTableFromMydb(r_review_condition, new MutableBoolean(false)));
		Dataset<Review> lonelyReview = fullOuterJoinsReview(lonelyReviewList);
		if(lonelyReview != null) {
			res = fullLeftOuterJoinsReview(Arrays.asList(res, lonelyReview));
		}
		if(r_review_refilter.booleanValue())
			res = res.filter((FilterFunction<Review>) r -> r_review_condition == null || r_review_condition.evaluate(r));
		
	
		return res;
		}
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
	public Dataset<Review> getR_review1ListInReviewUser(conditions.Condition<conditions.UserAttribute> r_author_condition,conditions.Condition<conditions.ReviewAttribute> r_review1_condition)		{
		MutableBoolean r_review1_refilter = new MutableBoolean(false);
		List<Dataset<Review>> datasetsPOJO = new ArrayList<Dataset<Review>>();
		Dataset<User> all = new UserServiceImpl().getUserList(r_author_condition);
		boolean all_already_persisted = false;
		MutableBoolean r_author_refilter;
		org.apache.spark.sql.Column joinCondition = null;
		
		
		Dataset<ReviewUser> res_reviewUser_r_review1;
		Dataset<Review> res_Review;
		// Role 'r_review1' mapped to EmbeddedObject 'author' - 'User' containing 'Review'
		r_author_refilter = new MutableBoolean(false);
		res_reviewUser_r_review1 = reviewUserService.getReviewUserListInIMDB_MongoreviewColauthor(r_review1_condition, r_author_condition, r_review1_refilter, r_author_refilter);
		if(r_author_refilter.booleanValue()) {
			joinCondition = null;
			joinCondition = res_reviewUser_r_review1.col("r_author.id").equalTo(all.col("id"));
			if(joinCondition == null)
				res_Review = res_reviewUser_r_review1.join(all).select("r_review1.*").as(Encoders.bean(Review.class));
			else
				res_Review = res_reviewUser_r_review1.join(all, joinCondition).select("r_review1.*").as(Encoders.bean(Review.class));
		
		} else
			res_Review = res_reviewUser_r_review1.map((MapFunction<ReviewUser,Review>) r -> r.getR_review1(), Encoders.bean(Review.class));
		res_Review = res_Review.dropDuplicates(new String[] {"id"});
		datasetsPOJO.add(res_Review);
		
		
		//Join datasets or return 
		Dataset<Review> res = fullOuterJoinsReview(datasetsPOJO);
		if(res == null)
			return null;
	
		List<Dataset<Review>> lonelyReviewList = new ArrayList<Dataset<Review>>();
		lonelyReviewList.add(getReviewListInReviewTableFromMydb(r_review1_condition, new MutableBoolean(false)));
		Dataset<Review> lonelyReview = fullOuterJoinsReview(lonelyReviewList);
		if(lonelyReview != null) {
			res = fullLeftOuterJoinsReview(Arrays.asList(res, lonelyReview));
		}
		if(r_review1_refilter.booleanValue())
			res = res.filter((FilterFunction<Review>) r -> r_review1_condition == null || r_review1_condition.evaluate(r));
		
	
		return res;
		}
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
	public boolean insertReview(
		Review review,
		Movie	r_reviewed_movieMovieReview,
		User	r_authorReviewUser){
			boolean inserted = false;
			// Insert in structures containing double embedded role
		    inserted = insertReviewInReviewColFromMymongo(review,r_reviewed_movieMovieReview,r_authorReviewUser) || inserted ;
			// Insert in descending structures
			// Insert in ascending structures 
			// Insert in ref structures 
			// Insert in standalone structures
			inserted = insertReviewInReviewTableFromMydb(review)|| inserted ;
			return inserted;
		}
	
	public boolean insertReviewInReviewTableFromMydb(Review review)	{
		Condition<ReviewAttribute> conditionID;
		String idvalue="";
		conditionID = Condition.simple(ReviewAttribute.id, Operator.EQUALS, review.getId());
		idvalue+=review.getId();
		boolean entityExists=false;
		entityExists = !getReviewListInReviewTableFromMydb(conditionID,new MutableBoolean(false)).isEmpty();
				
		if(!entityExists){
		List<Row> listRows=new ArrayList<Row>();
		List<Object> valuesreviewTable_1 = new ArrayList<>();
		List<StructField> listOfStructFieldreviewTable_1 = new ArrayList<StructField>();
		if(!listOfStructFieldreviewTable_1.contains(DataTypes.createStructField("id",DataTypes.StringType, true)))
			listOfStructFieldreviewTable_1.add(DataTypes.createStructField("id",DataTypes.StringType, true));
		valuesreviewTable_1.add(review.getId());
		if(!listOfStructFieldreviewTable_1.contains(DataTypes.createStructField("content",DataTypes.StringType, true)))
			listOfStructFieldreviewTable_1.add(DataTypes.createStructField("content",DataTypes.StringType, true));
		valuesreviewTable_1.add(review.getContent());
		
		StructType structType = DataTypes.createStructType(listOfStructFieldreviewTable_1);
		listRows.add(RowFactory.create(valuesreviewTable_1.toArray()));
		SparkConnectionMgr.writeDataset(listRows, structType, "jdbc", "reviewTable", "mydb");
			logger.info("Inserted [Review] entity ID [{}] in [ReviewTable] in database [Mydb]", idvalue);
		}
		else
			logger.warn("[Review] entity ID [{}] already present in [ReviewTable] in database [Mydb]", idvalue);
		return !entityExists;
	} 
	
	public boolean insertReviewInReviewColFromMymongo(Review review,
		Movie	r_reviewed_movieMovieReview,
		User	r_authorReviewUser)	{
			Document docreviewCol_1 = new Document();
			docreviewCol_1.append("idreview",review.getId());
			docreviewCol_1.append("content",review.getContent());
			docreviewCol_1.append("note",review.getNote());
			// field 'author' is mapped to mandatory role 'r_review1' with opposite role of type 'User'
					User user = r_authorReviewUser;
					Document docauthor_2 = new Document();
					docauthor_2.append("authorid",user.getId());
					docauthor_2.append("username",user.getUsername());
					docauthor_2.append("city",user.getCity());
					
					docreviewCol_1.append("author", docauthor_2);
			// field 'movie' is mapped to mandatory role 'r_review' with opposite role of type 'Movie'
					Movie movie = r_reviewed_movieMovieReview;
					Document docmovie_2 = new Document();
					docmovie_2.append("movieid",movie.getId());
					docmovie_2.append("title",movie.getPrimaryTitle());
					docmovie_2.append("avgrating",movie.getAverageRating());
					// field 'actors' is mapped to mandatory role 'movie' with opposite role of type 'Actor'
						List<Actor> characterMovieActor = movie._getCharacterList();
						if(characterMovieActor==null){
							logger.error("Physical Structure contains embedded attributes or reference to an inderectly linked object. Please set role attribute 'character' of type 'Actor' in entity object 'Movie");
							throw new PhysicalStructureException("Physical Structure contains embedded attributes or reference to an inderectly linked object. Please set role attribute 'character' of type 'Actor' in entity object 'Movie");
						}
						List<Document> arrayactors_2 = new ArrayList();
							for(Actor actor : characterMovieActor){
								Document docactors_3 = new Document();
								docactors_3.append("id",actor.getId());
								docactors_3.append("name",actor.getFullName());
								
								arrayactors_2.add(docactors_3);
							}
						docmovie_2.append("actors", arrayactors_2);
					
					docreviewCol_1.append("movie", docmovie_2);
			
			DBConnectionMgr.insertMany(List.of(docreviewCol_1), "reviewCol", "mymongo");
			return true;
			
		}
	public void updateReviewList(conditions.Condition<conditions.ReviewAttribute> condition, conditions.SetClause<conditions.ReviewAttribute> set){
		//TODO
	}
	
	public void updateReview(pojo.Review review) {
		//TODO using the id
		return;
	}
	public void updateR_reviewListInMovieReview(
		conditions.Condition<conditions.MovieAttribute> r_reviewed_movie_condition,
		conditions.Condition<conditions.ReviewAttribute> r_review_condition,
		
		conditions.SetClause<conditions.ReviewAttribute> set
	){
		//TODO
	}
	
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
	public void updateR_review1ListInReviewUser(
		conditions.Condition<conditions.UserAttribute> r_author_condition,
		conditions.Condition<conditions.ReviewAttribute> r_review1_condition,
		
		conditions.SetClause<conditions.ReviewAttribute> set
	){
		//TODO
	}
	
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
	
	
	public void deleteReviewList(conditions.Condition<conditions.ReviewAttribute> condition){
		//TODO
	}
	
	public void deleteReview(pojo.Review review) {
		//TODO using the id
		return;
	}
	public void deleteR_reviewListInMovieReview(	
		conditions.Condition<conditions.MovieAttribute> r_reviewed_movie_condition,	
		conditions.Condition<conditions.ReviewAttribute> r_review_condition){
			//TODO
		}
	
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
	public void deleteR_review1ListInReviewUser(	
		conditions.Condition<conditions.UserAttribute> r_author_condition,	
		conditions.Condition<conditions.ReviewAttribute> r_review1_condition){
			//TODO
		}
	
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
