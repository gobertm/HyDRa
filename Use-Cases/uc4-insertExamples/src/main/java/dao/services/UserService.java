package dao.services;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pojo.User;
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
import conditions.UserAttribute;
import conditions.ReviewAttribute;
import pojo.Review;

public abstract class UserService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(UserService.class);
	protected ReviewUserService reviewUserService = new dao.impl.ReviewUserServiceImpl();
	


	public static enum ROLE_NAME {
		REVIEWUSER_R_AUTHOR
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.REVIEWUSER_R_AUTHOR, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public UserService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public UserService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<User> getUserList(conditions.Condition<conditions.UserAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<User>> datasets = new ArrayList<Dataset<User>>();
		Dataset<User> d = null;
		d = getUserListInReviewColFromMymongo(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsUser(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<User>) r -> condition == null || condition.evaluate(r));
		d=d.dropDuplicates();
		return d;
	}
	
	
	
	
	public abstract Dataset<User> getUserListInReviewColFromMymongo(conditions.Condition<conditions.UserAttribute> condition, MutableBoolean refilterFlag);
	
	
	// TODO get based on id(s). Ex:public Client getClientById(Long id)
	
	public Dataset<User> getUserListById(String id) {
		return getUserList(conditions.Condition.simple(conditions.UserAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<User> getUserListByUsername(String username) {
		return getUserList(conditions.Condition.simple(conditions.UserAttribute.username, conditions.Operator.EQUALS, username));
	}
	
	public Dataset<User> getUserListByCity(String city) {
		return getUserList(conditions.Condition.simple(conditions.UserAttribute.city, conditions.Operator.EQUALS, city));
	}
	
	
	
	protected static Dataset<User> fullOuterJoinsUser(List<Dataset<User>> datasetsPOJO) {
		return fullOuterJoinsUser(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<User> fullLeftOuterJoinsUser(List<Dataset<User>> datasetsPOJO) {
		return fullOuterJoinsUser(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<User> fullOuterJoinsUser(List<Dataset<User>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<User> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("username", "username_1")
								.withColumnRenamed("city", "city_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("username", "username_" + i)
								.withColumnRenamed("city", "city_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			} 
			d = res.map((MapFunction<Row, User>) r -> {
					User user_res = new User();
					
					// attribute 'User.id'
					String firstNotNull_id = Util.getStringValue(r.getAs("id"));
					user_res.setId(firstNotNull_id);
					
					// attribute 'User.username'
					String firstNotNull_username = Util.getStringValue(r.getAs("username"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String username2 = Util.getStringValue(r.getAs("username_" + i));
						if (firstNotNull_username != null && username2 != null && !firstNotNull_username.equals(username2)) {
							user_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'User.username': " + firstNotNull_username + " and " + username2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'User.username' ==> " + firstNotNull_username + " and " + username2);
						}
						if (firstNotNull_username == null && username2 != null) {
							firstNotNull_username = username2;
						}
					}
					user_res.setUsername(firstNotNull_username);
					
					// attribute 'User.city'
					String firstNotNull_city = Util.getStringValue(r.getAs("city"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String city2 = Util.getStringValue(r.getAs("city_" + i));
						if (firstNotNull_city != null && city2 != null && !firstNotNull_city.equals(city2)) {
							user_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'User.city': " + firstNotNull_city + " and " + city2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'User.city' ==> " + firstNotNull_city + " and " + city2);
						}
						if (firstNotNull_city == null && city2 != null) {
							firstNotNull_city = city2;
						}
					}
					user_res.setCity(firstNotNull_city);
	
					scala.collection.mutable.WrappedArray<String> logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							user_res.addLogEvent(logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							user_res.addLogEvent(logEvents.apply(j));
						}
					}
	
					return user_res;
				}, Encoders.bean(User.class));
			return d;
	}
	
	
	
	
	
	public User getUser(User.reviewUser role, Review review) {
		if(role != null) {
			if(role.equals(User.reviewUser.r_author))
				return getR_authorInReviewUserByR_review1(review);
		}
		return null;
	}
	
	public Dataset<User> getUserList(User.reviewUser role, Condition<ReviewAttribute> condition) {
		if(role != null) {
			if(role.equals(User.reviewUser.r_author))
				return getR_authorListInReviewUserByR_review1Condition(condition);
		}
		return null;
	}
	
	public Dataset<User> getUserList(User.reviewUser role, Condition<UserAttribute> condition1, Condition<ReviewAttribute> condition2) {
		if(role != null) {
			if(role.equals(User.reviewUser.r_author))
				return getR_authorListInReviewUser(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	public abstract Dataset<User> getR_authorListInReviewUser(conditions.Condition<conditions.UserAttribute> r_author_condition,conditions.Condition<conditions.ReviewAttribute> r_review1_condition);
	
	public Dataset<User> getR_authorListInReviewUserByR_authorCondition(conditions.Condition<conditions.UserAttribute> r_author_condition){
		return getR_authorListInReviewUser(r_author_condition, null);
	}
	public Dataset<User> getR_authorListInReviewUserByR_review1Condition(conditions.Condition<conditions.ReviewAttribute> r_review1_condition){
		return getR_authorListInReviewUser(null, r_review1_condition);
	}
	
	public User getR_authorInReviewUserByR_review1(pojo.Review r_review1){
		if(r_review1 == null)
			return null;
	
		Condition c;
		c=Condition.simple(ReviewAttribute.id,Operator.EQUALS, r_review1.getId());
		Dataset<User> res = getR_authorListInReviewUserByR_review1Condition(c);
		return !res.isEmpty()?res.first():null;
	}
	
	
	public abstract boolean insertUser(User user);
	
	
	
	public abstract void updateUserList(conditions.Condition<conditions.UserAttribute> condition, conditions.SetClause<conditions.UserAttribute> set);
	
	public void updateUser(pojo.User user) {
		//TODO using the id
		return;
	}
	public abstract void updateR_authorListInReviewUser(
		conditions.Condition<conditions.UserAttribute> r_author_condition,
		conditions.Condition<conditions.ReviewAttribute> r_review1_condition,
		
		conditions.SetClause<conditions.UserAttribute> set
	);
	
	public void updateR_authorListInReviewUserByR_authorCondition(
		conditions.Condition<conditions.UserAttribute> r_author_condition,
		conditions.SetClause<conditions.UserAttribute> set
	){
		updateR_authorListInReviewUser(r_author_condition, null, set);
	}
	public void updateR_authorListInReviewUserByR_review1Condition(
		conditions.Condition<conditions.ReviewAttribute> r_review1_condition,
		conditions.SetClause<conditions.UserAttribute> set
	){
		updateR_authorListInReviewUser(null, r_review1_condition, set);
	}
	
	public void updateR_authorInReviewUserByR_review1(
		pojo.Review r_review1,
		conditions.SetClause<conditions.UserAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	
	
	public abstract void deleteUserList(conditions.Condition<conditions.UserAttribute> condition);
	
	public void deleteUser(pojo.User user) {
		//TODO using the id
		return;
	}
	public abstract void deleteR_authorListInReviewUser(	
		conditions.Condition<conditions.UserAttribute> r_author_condition,	
		conditions.Condition<conditions.ReviewAttribute> r_review1_condition);
	
	public void deleteR_authorListInReviewUserByR_authorCondition(
		conditions.Condition<conditions.UserAttribute> r_author_condition
	){
		deleteR_authorListInReviewUser(r_author_condition, null);
	}
	public void deleteR_authorListInReviewUserByR_review1Condition(
		conditions.Condition<conditions.ReviewAttribute> r_review1_condition
	){
		deleteR_authorListInReviewUser(null, r_review1_condition);
	}
	
	public void deleteR_authorInReviewUserByR_review1(
		pojo.Review r_review1 
	){
		//TODO get id in condition
		return;	
	}
	
	
}
