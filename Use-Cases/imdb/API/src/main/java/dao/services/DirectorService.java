package dao.services;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pojo.Director;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.commons.lang.mutable.MutableBoolean;
import conditions.Condition;
import conditions.Operator;
import conditions.DirectorAttribute;
import conditions.MovieAttribute;
import pojo.Movie;

public abstract class DirectorService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DirectorService.class);
	protected MovieDirectorService movieDirectorService = new dao.impl.MovieDirectorServiceImpl();
	


	public static enum ROLE_NAME {
		MOVIEDIRECTOR_DIRECTOR
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.MOVIEDIRECTOR_DIRECTOR, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public DirectorService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public DirectorService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<Director> getDirectorList(conditions.Condition<conditions.DirectorAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Director>> datasets = new ArrayList<Dataset<Director>>();
		Dataset<Director> d = null;
		d = getDirectorListInDirectorTableFromMydb(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
	
		
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasets.get(1)
								.withColumnRenamed("firstName", "firstName_1")
								.withColumnRenamed("lastName", "lastName_1")
								.withColumnRenamed("yearOfBirth", "yearOfBirth_1")
								.withColumnRenamed("yearOfDeath", "yearOfDeath_1")
							, seq, "fullouter");
			for(int i = 2; i < datasets.size(); i++) {
				res = res.join(datasets.get(i)
								.withColumnRenamed("firstName", "firstName_" + i)
								.withColumnRenamed("lastName", "lastName_" + i)
								.withColumnRenamed("yearOfBirth", "yearOfBirth_" + i)
								.withColumnRenamed("yearOfDeath", "yearOfDeath_" + i)
							, seq, "fullouter");
			} 
			d = res.map((MapFunction<Row, Director>) r -> {
					Director director_res = new Director();
					
					// attribute 'Director.id'
					String firstNotNull_id = r.getAs("id");
					director_res.setId(firstNotNull_id);
					
					// attribute 'Director.firstName'
					String firstNotNull_firstName = r.getAs("firstName");
					for (int i = 1; i < datasets.size(); i++) {
						String firstName2 = r.getAs("firstName_" + i);
						if (firstNotNull_firstName != null && firstName2 != null && !firstNotNull_firstName.equals(firstName2)) {
							director_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Director.firstName': " + firstNotNull_firstName + " and " + firstName2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Director.firstName' ==> " + firstNotNull_firstName + " and " + firstName2);
						}
						if (firstNotNull_firstName == null && firstName2 != null) {
							firstNotNull_firstName = firstName2;
						}
					}
					director_res.setFirstName(firstNotNull_firstName);
					
					// attribute 'Director.lastName'
					String firstNotNull_lastName = r.getAs("lastName");
					for (int i = 1; i < datasets.size(); i++) {
						String lastName2 = r.getAs("lastName_" + i);
						if (firstNotNull_lastName != null && lastName2 != null && !firstNotNull_lastName.equals(lastName2)) {
							director_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Director.lastName': " + firstNotNull_lastName + " and " + lastName2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Director.lastName' ==> " + firstNotNull_lastName + " and " + lastName2);
						}
						if (firstNotNull_lastName == null && lastName2 != null) {
							firstNotNull_lastName = lastName2;
						}
					}
					director_res.setLastName(firstNotNull_lastName);
					
					// attribute 'Director.yearOfBirth'
					Integer firstNotNull_yearOfBirth = r.getAs("yearOfBirth");
					for (int i = 1; i < datasets.size(); i++) {
						Integer yearOfBirth2 = r.getAs("yearOfBirth_" + i);
						if (firstNotNull_yearOfBirth != null && yearOfBirth2 != null && !firstNotNull_yearOfBirth.equals(yearOfBirth2)) {
							director_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Director.yearOfBirth': " + firstNotNull_yearOfBirth + " and " + yearOfBirth2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Director.yearOfBirth' ==> " + firstNotNull_yearOfBirth + " and " + yearOfBirth2);
						}
						if (firstNotNull_yearOfBirth == null && yearOfBirth2 != null) {
							firstNotNull_yearOfBirth = yearOfBirth2;
						}
					}
					director_res.setYearOfBirth(firstNotNull_yearOfBirth);
					
					// attribute 'Director.yearOfDeath'
					Integer firstNotNull_yearOfDeath = r.getAs("yearOfDeath");
					for (int i = 1; i < datasets.size(); i++) {
						Integer yearOfDeath2 = r.getAs("yearOfDeath_" + i);
						if (firstNotNull_yearOfDeath != null && yearOfDeath2 != null && !firstNotNull_yearOfDeath.equals(yearOfDeath2)) {
							director_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Director.yearOfDeath': " + firstNotNull_yearOfDeath + " and " + yearOfDeath2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Director.yearOfDeath' ==> " + firstNotNull_yearOfDeath + " and " + yearOfDeath2);
						}
						if (firstNotNull_yearOfDeath == null && yearOfDeath2 != null) {
							firstNotNull_yearOfDeath = yearOfDeath2;
						}
					}
					director_res.setYearOfDeath(firstNotNull_yearOfDeath);
					return director_res;
				}, Encoders.bean(Director.class));
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Director>) r -> condition == null || condition.evaluate(r));
		d=d.dropDuplicates();
		return d;
	}
	
	
	
	public abstract Dataset<Director> getDirectorListInDirectorTableFromMydb(conditions.Condition<conditions.DirectorAttribute> condition, MutableBoolean refilterFlag);
	
	
	// TODO get based on id(s). Ex:public Client getClientById(Long id)
	
	public Dataset<Director> getDirectorListById(String id) {
		return getDirectorList(conditions.Condition.simple(conditions.DirectorAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Director> getDirectorListByFirstName(String firstName) {
		return getDirectorList(conditions.Condition.simple(conditions.DirectorAttribute.firstName, conditions.Operator.EQUALS, firstName));
	}
	
	public Dataset<Director> getDirectorListByLastName(String lastName) {
		return getDirectorList(conditions.Condition.simple(conditions.DirectorAttribute.lastName, conditions.Operator.EQUALS, lastName));
	}
	
	public Dataset<Director> getDirectorListByYearOfBirth(Integer yearOfBirth) {
		return getDirectorList(conditions.Condition.simple(conditions.DirectorAttribute.yearOfBirth, conditions.Operator.EQUALS, yearOfBirth));
	}
	
	public Dataset<Director> getDirectorListByYearOfDeath(Integer yearOfDeath) {
		return getDirectorList(conditions.Condition.simple(conditions.DirectorAttribute.yearOfDeath, conditions.Operator.EQUALS, yearOfDeath));
	}
	
	
	
	protected static Dataset<Director> fullOuterJoinsDirector(List<Dataset<Director>> datasetsPOJO) {
		return fullOuterJoinsDirector(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Director> fullLeftOuterJoinsDirector(List<Dataset<Director>> datasetsPOJO) {
		return fullOuterJoinsDirector(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Director> fullOuterJoinsDirector(List<Dataset<Director>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Director> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("firstName", "firstName_1")
								.withColumnRenamed("lastName", "lastName_1")
								.withColumnRenamed("yearOfBirth", "yearOfBirth_1")
								.withColumnRenamed("yearOfDeath", "yearOfDeath_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("firstName", "firstName_" + i)
								.withColumnRenamed("lastName", "lastName_" + i)
								.withColumnRenamed("yearOfBirth", "yearOfBirth_" + i)
								.withColumnRenamed("yearOfDeath", "yearOfDeath_" + i)
							, seq, joinMode);
			} 
			d = res.map((MapFunction<Row, Director>) r -> {
					Director director_res = new Director();
					
					// attribute 'Director.id'
					String firstNotNull_id = r.getAs("id");
					director_res.setId(firstNotNull_id);
					
					// attribute 'Director.firstName'
					String firstNotNull_firstName = r.getAs("firstName");
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String firstName2 = r.getAs("firstName_" + i);
						if (firstNotNull_firstName != null && firstName2 != null && !firstNotNull_firstName.equals(firstName2)) {
							director_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Director.firstName': " + firstNotNull_firstName + " and " + firstName2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Director.firstName' ==> " + firstNotNull_firstName + " and " + firstName2);
						}
						if (firstNotNull_firstName == null && firstName2 != null) {
							firstNotNull_firstName = firstName2;
						}
					}
					director_res.setFirstName(firstNotNull_firstName);
					
					// attribute 'Director.lastName'
					String firstNotNull_lastName = r.getAs("lastName");
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String lastName2 = r.getAs("lastName_" + i);
						if (firstNotNull_lastName != null && lastName2 != null && !firstNotNull_lastName.equals(lastName2)) {
							director_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Director.lastName': " + firstNotNull_lastName + " and " + lastName2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Director.lastName' ==> " + firstNotNull_lastName + " and " + lastName2);
						}
						if (firstNotNull_lastName == null && lastName2 != null) {
							firstNotNull_lastName = lastName2;
						}
					}
					director_res.setLastName(firstNotNull_lastName);
					
					// attribute 'Director.yearOfBirth'
					Integer firstNotNull_yearOfBirth = r.getAs("yearOfBirth");
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer yearOfBirth2 = r.getAs("yearOfBirth_" + i);
						if (firstNotNull_yearOfBirth != null && yearOfBirth2 != null && !firstNotNull_yearOfBirth.equals(yearOfBirth2)) {
							director_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Director.yearOfBirth': " + firstNotNull_yearOfBirth + " and " + yearOfBirth2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Director.yearOfBirth' ==> " + firstNotNull_yearOfBirth + " and " + yearOfBirth2);
						}
						if (firstNotNull_yearOfBirth == null && yearOfBirth2 != null) {
							firstNotNull_yearOfBirth = yearOfBirth2;
						}
					}
					director_res.setYearOfBirth(firstNotNull_yearOfBirth);
					
					// attribute 'Director.yearOfDeath'
					Integer firstNotNull_yearOfDeath = r.getAs("yearOfDeath");
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer yearOfDeath2 = r.getAs("yearOfDeath_" + i);
						if (firstNotNull_yearOfDeath != null && yearOfDeath2 != null && !firstNotNull_yearOfDeath.equals(yearOfDeath2)) {
							director_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Director.yearOfDeath': " + firstNotNull_yearOfDeath + " and " + yearOfDeath2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Director.yearOfDeath' ==> " + firstNotNull_yearOfDeath + " and " + yearOfDeath2);
						}
						if (firstNotNull_yearOfDeath == null && yearOfDeath2 != null) {
							firstNotNull_yearOfDeath = yearOfDeath2;
						}
					}
					director_res.setYearOfDeath(firstNotNull_yearOfDeath);
					return director_res;
				}, Encoders.bean(Director.class));
			return d;
	}
	
	
	public Dataset<Director> getDirectorList(Director.movieDirector role, Movie movie) {
		if(role != null) {
			if(role.equals(Director.movieDirector.director))
				return getDirectorListInMovieDirectorByDirected_movie(movie);
		}
		return null;
	}
	
	public Dataset<Director> getDirectorList(Director.movieDirector role, Condition<MovieAttribute> condition) {
		if(role != null) {
			if(role.equals(Director.movieDirector.director))
				return getDirectorListInMovieDirectorByDirected_movieCondition(condition);
		}
		return null;
	}
	
	public Dataset<Director> getDirectorList(Director.movieDirector role, Condition<MovieAttribute> condition1, Condition<DirectorAttribute> condition2) {
		if(role != null) {
			if(role.equals(Director.movieDirector.director))
				return getDirectorListInMovieDirector(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	
	public abstract Dataset<Director> getDirectorListInMovieDirector(conditions.Condition<conditions.MovieAttribute> directed_movie_condition,conditions.Condition<conditions.DirectorAttribute> director_condition);
	
	public Dataset<Director> getDirectorListInMovieDirectorByDirected_movieCondition(conditions.Condition<conditions.MovieAttribute> directed_movie_condition){
		return getDirectorListInMovieDirector(directed_movie_condition, null);
	}
	
	public Dataset<Director> getDirectorListInMovieDirectorByDirected_movie(pojo.Movie directed_movie){
		if(directed_movie == null)
			return null;
	
		Condition c;
		c=Condition.simple(MovieAttribute.id,Operator.EQUALS, directed_movie.getId());
		Dataset<Director> res = getDirectorListInMovieDirectorByDirected_movieCondition(c);
		return res;
	}
	
	public Dataset<Director> getDirectorListInMovieDirectorByDirectorCondition(conditions.Condition<conditions.DirectorAttribute> director_condition){
		return getDirectorListInMovieDirector(null, director_condition);
	}
	
	public abstract void insertDirectorAndLinkedItems(Director director);
	public abstract void insertDirector(Director director);
	
	public abstract void insertDirectorInDirectorTableFromMydb(Director director); 
	public abstract void updateDirectorList(conditions.Condition<conditions.DirectorAttribute> condition, conditions.SetClause<conditions.DirectorAttribute> set);
	
	public void updateDirector(pojo.Director director) {
		//TODO using the id
		return;
	}
	public abstract void updateDirectorListInMovieDirector(
		conditions.Condition<conditions.MovieAttribute> directed_movie_condition,
		conditions.Condition<conditions.DirectorAttribute> director_condition,
		
		conditions.SetClause<conditions.DirectorAttribute> set
	);
	
	public void updateDirectorListInMovieDirectorByDirected_movieCondition(
		conditions.Condition<conditions.MovieAttribute> directed_movie_condition,
		conditions.SetClause<conditions.DirectorAttribute> set
	){
		updateDirectorListInMovieDirector(directed_movie_condition, null, set);
	}
	
	public void updateDirectorListInMovieDirectorByDirected_movie(
		pojo.Movie directed_movie,
		conditions.SetClause<conditions.DirectorAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateDirectorListInMovieDirectorByDirectorCondition(
		conditions.Condition<conditions.DirectorAttribute> director_condition,
		conditions.SetClause<conditions.DirectorAttribute> set
	){
		updateDirectorListInMovieDirector(null, director_condition, set);
	}
	
	
	public abstract void deleteDirectorList(conditions.Condition<conditions.DirectorAttribute> condition);
	
	public void deleteDirector(pojo.Director director) {
		//TODO using the id
		return;
	}
	public abstract void deleteDirectorListInMovieDirector(	
		conditions.Condition<conditions.MovieAttribute> directed_movie_condition,	
		conditions.Condition<conditions.DirectorAttribute> director_condition);
	
	public void deleteDirectorListInMovieDirectorByDirected_movieCondition(
		conditions.Condition<conditions.MovieAttribute> directed_movie_condition
	){
		deleteDirectorListInMovieDirector(directed_movie_condition, null);
	}
	
	public void deleteDirectorListInMovieDirectorByDirected_movie(
		pojo.Movie directed_movie 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteDirectorListInMovieDirectorByDirectorCondition(
		conditions.Condition<conditions.DirectorAttribute> director_condition
	){
		deleteDirectorListInMovieDirector(null, director_condition);
	}
	
}
