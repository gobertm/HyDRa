package dao.services;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pojo.Movie;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.commons.lang.mutable.MutableBoolean;
import conditions.Condition;
import conditions.Operator;
import conditions.MovieAttribute;
import conditions.DirectorAttribute;
import pojo.Director;
import conditions.MovieAttribute;
import conditions.ActorAttribute;
import pojo.Actor;

public abstract class MovieService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MovieService.class);
	protected MovieDirectorService movieDirectorService = new dao.impl.MovieDirectorServiceImpl();
	protected MovieActorService movieActorService = new dao.impl.MovieActorServiceImpl();
	


	public static enum ROLE_NAME {
		MOVIEDIRECTOR_DIRECTED_MOVIE, MOVIEACTOR_MOVIE
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.MOVIEDIRECTOR_DIRECTED_MOVIE, loading.Loading.LAZY);
		defaultLoadingParameters.put(ROLE_NAME.MOVIEACTOR_MOVIE, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public MovieService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public MovieService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<Movie> getMovieList(conditions.Condition<conditions.MovieAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Movie>> datasets = new ArrayList<Dataset<Movie>>();
		Dataset<Movie> d = null;
		d = getMovieListInActorCollectionFromMymongo(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		d = getMovieListInMovieKVFromMyredis(condition, refilterFlag);
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
								.withColumnRenamed("primaryTitle", "primaryTitle_1")
								.withColumnRenamed("originalTitle", "originalTitle_1")
								.withColumnRenamed("isAdult", "isAdult_1")
								.withColumnRenamed("startYear", "startYear_1")
								.withColumnRenamed("runtimeMinutes", "runtimeMinutes_1")
								.withColumnRenamed("averageRating", "averageRating_1")
								.withColumnRenamed("numVotes", "numVotes_1")
							, seq, "fullouter");
			for(int i = 2; i < datasets.size(); i++) {
				res = res.join(datasets.get(i)
								.withColumnRenamed("primaryTitle", "primaryTitle_" + i)
								.withColumnRenamed("originalTitle", "originalTitle_" + i)
								.withColumnRenamed("isAdult", "isAdult_" + i)
								.withColumnRenamed("startYear", "startYear_" + i)
								.withColumnRenamed("runtimeMinutes", "runtimeMinutes_" + i)
								.withColumnRenamed("averageRating", "averageRating_" + i)
								.withColumnRenamed("numVotes", "numVotes_" + i)
							, seq, "fullouter");
			} 
			d = res.map((MapFunction<Row, Movie>) r -> {
					Movie movie_res = new Movie();
					
					// attribute 'Movie.id'
					String firstNotNull_id = r.getAs("id");
					movie_res.setId(firstNotNull_id);
					
					// attribute 'Movie.primaryTitle'
					String firstNotNull_primaryTitle = r.getAs("primaryTitle");
					for (int i = 1; i < datasets.size(); i++) {
						String primaryTitle2 = r.getAs("primaryTitle_" + i);
						if (firstNotNull_primaryTitle != null && primaryTitle2 != null && !firstNotNull_primaryTitle.equals(primaryTitle2)) {
							movie_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Movie.primaryTitle': " + firstNotNull_primaryTitle + " and " + primaryTitle2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Movie.primaryTitle' ==> " + firstNotNull_primaryTitle + " and " + primaryTitle2);
						}
						if (firstNotNull_primaryTitle == null && primaryTitle2 != null) {
							firstNotNull_primaryTitle = primaryTitle2;
						}
					}
					movie_res.setPrimaryTitle(firstNotNull_primaryTitle);
					
					// attribute 'Movie.originalTitle'
					String firstNotNull_originalTitle = r.getAs("originalTitle");
					for (int i = 1; i < datasets.size(); i++) {
						String originalTitle2 = r.getAs("originalTitle_" + i);
						if (firstNotNull_originalTitle != null && originalTitle2 != null && !firstNotNull_originalTitle.equals(originalTitle2)) {
							movie_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Movie.originalTitle': " + firstNotNull_originalTitle + " and " + originalTitle2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Movie.originalTitle' ==> " + firstNotNull_originalTitle + " and " + originalTitle2);
						}
						if (firstNotNull_originalTitle == null && originalTitle2 != null) {
							firstNotNull_originalTitle = originalTitle2;
						}
					}
					movie_res.setOriginalTitle(firstNotNull_originalTitle);
					
					// attribute 'Movie.isAdult'
					Boolean firstNotNull_isAdult = r.getAs("isAdult");
					for (int i = 1; i < datasets.size(); i++) {
						Boolean isAdult2 = r.getAs("isAdult_" + i);
						if (firstNotNull_isAdult != null && isAdult2 != null && !firstNotNull_isAdult.equals(isAdult2)) {
							movie_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Movie.isAdult': " + firstNotNull_isAdult + " and " + isAdult2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Movie.isAdult' ==> " + firstNotNull_isAdult + " and " + isAdult2);
						}
						if (firstNotNull_isAdult == null && isAdult2 != null) {
							firstNotNull_isAdult = isAdult2;
						}
					}
					movie_res.setIsAdult(firstNotNull_isAdult);
					
					// attribute 'Movie.startYear'
					Integer firstNotNull_startYear = r.getAs("startYear");
					for (int i = 1; i < datasets.size(); i++) {
						Integer startYear2 = r.getAs("startYear_" + i);
						if (firstNotNull_startYear != null && startYear2 != null && !firstNotNull_startYear.equals(startYear2)) {
							movie_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Movie.startYear': " + firstNotNull_startYear + " and " + startYear2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Movie.startYear' ==> " + firstNotNull_startYear + " and " + startYear2);
						}
						if (firstNotNull_startYear == null && startYear2 != null) {
							firstNotNull_startYear = startYear2;
						}
					}
					movie_res.setStartYear(firstNotNull_startYear);
					
					// attribute 'Movie.runtimeMinutes'
					Integer firstNotNull_runtimeMinutes = r.getAs("runtimeMinutes");
					for (int i = 1; i < datasets.size(); i++) {
						Integer runtimeMinutes2 = r.getAs("runtimeMinutes_" + i);
						if (firstNotNull_runtimeMinutes != null && runtimeMinutes2 != null && !firstNotNull_runtimeMinutes.equals(runtimeMinutes2)) {
							movie_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Movie.runtimeMinutes': " + firstNotNull_runtimeMinutes + " and " + runtimeMinutes2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Movie.runtimeMinutes' ==> " + firstNotNull_runtimeMinutes + " and " + runtimeMinutes2);
						}
						if (firstNotNull_runtimeMinutes == null && runtimeMinutes2 != null) {
							firstNotNull_runtimeMinutes = runtimeMinutes2;
						}
					}
					movie_res.setRuntimeMinutes(firstNotNull_runtimeMinutes);
					
					// attribute 'Movie.averageRating'
					String firstNotNull_averageRating = r.getAs("averageRating");
					for (int i = 1; i < datasets.size(); i++) {
						String averageRating2 = r.getAs("averageRating_" + i);
						if (firstNotNull_averageRating != null && averageRating2 != null && !firstNotNull_averageRating.equals(averageRating2)) {
							movie_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Movie.averageRating': " + firstNotNull_averageRating + " and " + averageRating2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Movie.averageRating' ==> " + firstNotNull_averageRating + " and " + averageRating2);
						}
						if (firstNotNull_averageRating == null && averageRating2 != null) {
							firstNotNull_averageRating = averageRating2;
						}
					}
					movie_res.setAverageRating(firstNotNull_averageRating);
					
					// attribute 'Movie.numVotes'
					Integer firstNotNull_numVotes = r.getAs("numVotes");
					for (int i = 1; i < datasets.size(); i++) {
						Integer numVotes2 = r.getAs("numVotes_" + i);
						if (firstNotNull_numVotes != null && numVotes2 != null && !firstNotNull_numVotes.equals(numVotes2)) {
							movie_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Movie.numVotes': " + firstNotNull_numVotes + " and " + numVotes2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Movie.numVotes' ==> " + firstNotNull_numVotes + " and " + numVotes2);
						}
						if (firstNotNull_numVotes == null && numVotes2 != null) {
							firstNotNull_numVotes = numVotes2;
						}
					}
					movie_res.setNumVotes(firstNotNull_numVotes);
					return movie_res;
				}, Encoders.bean(Movie.class));
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Movie>) r -> condition == null || condition.evaluate(r));
		d=d.dropDuplicates();
		return d;
	}
	
	
	
	public abstract Dataset<Movie> getMovieListInMovieKVFromMyredis(conditions.Condition<conditions.MovieAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	public abstract Dataset<Movie> getMovieListInActorCollectionFromMymongo(conditions.Condition<conditions.MovieAttribute> condition, MutableBoolean refilterFlag);
	
	
	// TODO get based on id(s). Ex:public Client getClientById(Long id)
	
	public Dataset<Movie> getMovieListById(String id) {
		return getMovieList(conditions.Condition.simple(conditions.MovieAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Movie> getMovieListByPrimaryTitle(String primaryTitle) {
		return getMovieList(conditions.Condition.simple(conditions.MovieAttribute.primaryTitle, conditions.Operator.EQUALS, primaryTitle));
	}
	
	public Dataset<Movie> getMovieListByOriginalTitle(String originalTitle) {
		return getMovieList(conditions.Condition.simple(conditions.MovieAttribute.originalTitle, conditions.Operator.EQUALS, originalTitle));
	}
	
	public Dataset<Movie> getMovieListByIsAdult(Boolean isAdult) {
		return getMovieList(conditions.Condition.simple(conditions.MovieAttribute.isAdult, conditions.Operator.EQUALS, isAdult));
	}
	
	public Dataset<Movie> getMovieListByStartYear(Integer startYear) {
		return getMovieList(conditions.Condition.simple(conditions.MovieAttribute.startYear, conditions.Operator.EQUALS, startYear));
	}
	
	public Dataset<Movie> getMovieListByRuntimeMinutes(Integer runtimeMinutes) {
		return getMovieList(conditions.Condition.simple(conditions.MovieAttribute.runtimeMinutes, conditions.Operator.EQUALS, runtimeMinutes));
	}
	
	public Dataset<Movie> getMovieListByAverageRating(String averageRating) {
		return getMovieList(conditions.Condition.simple(conditions.MovieAttribute.averageRating, conditions.Operator.EQUALS, averageRating));
	}
	
	public Dataset<Movie> getMovieListByNumVotes(Integer numVotes) {
		return getMovieList(conditions.Condition.simple(conditions.MovieAttribute.numVotes, conditions.Operator.EQUALS, numVotes));
	}
	
	
	
	protected static Dataset<Movie> fullOuterJoinsMovie(List<Dataset<Movie>> datasetsPOJO) {
		return fullOuterJoinsMovie(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Movie> fullLeftOuterJoinsMovie(List<Dataset<Movie>> datasetsPOJO) {
		return fullOuterJoinsMovie(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Movie> fullOuterJoinsMovie(List<Dataset<Movie>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Movie> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("primaryTitle", "primaryTitle_1")
								.withColumnRenamed("originalTitle", "originalTitle_1")
								.withColumnRenamed("isAdult", "isAdult_1")
								.withColumnRenamed("startYear", "startYear_1")
								.withColumnRenamed("runtimeMinutes", "runtimeMinutes_1")
								.withColumnRenamed("averageRating", "averageRating_1")
								.withColumnRenamed("numVotes", "numVotes_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("primaryTitle", "primaryTitle_" + i)
								.withColumnRenamed("originalTitle", "originalTitle_" + i)
								.withColumnRenamed("isAdult", "isAdult_" + i)
								.withColumnRenamed("startYear", "startYear_" + i)
								.withColumnRenamed("runtimeMinutes", "runtimeMinutes_" + i)
								.withColumnRenamed("averageRating", "averageRating_" + i)
								.withColumnRenamed("numVotes", "numVotes_" + i)
							, seq, joinMode);
			} 
			d = res.map((MapFunction<Row, Movie>) r -> {
					Movie movie_res = new Movie();
					
					// attribute 'Movie.id'
					String firstNotNull_id = r.getAs("id");
					movie_res.setId(firstNotNull_id);
					
					// attribute 'Movie.primaryTitle'
					String firstNotNull_primaryTitle = r.getAs("primaryTitle");
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String primaryTitle2 = r.getAs("primaryTitle_" + i);
						if (firstNotNull_primaryTitle != null && primaryTitle2 != null && !firstNotNull_primaryTitle.equals(primaryTitle2)) {
							movie_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Movie.primaryTitle': " + firstNotNull_primaryTitle + " and " + primaryTitle2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Movie.primaryTitle' ==> " + firstNotNull_primaryTitle + " and " + primaryTitle2);
						}
						if (firstNotNull_primaryTitle == null && primaryTitle2 != null) {
							firstNotNull_primaryTitle = primaryTitle2;
						}
					}
					movie_res.setPrimaryTitle(firstNotNull_primaryTitle);
					
					// attribute 'Movie.originalTitle'
					String firstNotNull_originalTitle = r.getAs("originalTitle");
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String originalTitle2 = r.getAs("originalTitle_" + i);
						if (firstNotNull_originalTitle != null && originalTitle2 != null && !firstNotNull_originalTitle.equals(originalTitle2)) {
							movie_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Movie.originalTitle': " + firstNotNull_originalTitle + " and " + originalTitle2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Movie.originalTitle' ==> " + firstNotNull_originalTitle + " and " + originalTitle2);
						}
						if (firstNotNull_originalTitle == null && originalTitle2 != null) {
							firstNotNull_originalTitle = originalTitle2;
						}
					}
					movie_res.setOriginalTitle(firstNotNull_originalTitle);
					
					// attribute 'Movie.isAdult'
					Boolean firstNotNull_isAdult = r.getAs("isAdult");
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Boolean isAdult2 = r.getAs("isAdult_" + i);
						if (firstNotNull_isAdult != null && isAdult2 != null && !firstNotNull_isAdult.equals(isAdult2)) {
							movie_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Movie.isAdult': " + firstNotNull_isAdult + " and " + isAdult2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Movie.isAdult' ==> " + firstNotNull_isAdult + " and " + isAdult2);
						}
						if (firstNotNull_isAdult == null && isAdult2 != null) {
							firstNotNull_isAdult = isAdult2;
						}
					}
					movie_res.setIsAdult(firstNotNull_isAdult);
					
					// attribute 'Movie.startYear'
					Integer firstNotNull_startYear = r.getAs("startYear");
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer startYear2 = r.getAs("startYear_" + i);
						if (firstNotNull_startYear != null && startYear2 != null && !firstNotNull_startYear.equals(startYear2)) {
							movie_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Movie.startYear': " + firstNotNull_startYear + " and " + startYear2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Movie.startYear' ==> " + firstNotNull_startYear + " and " + startYear2);
						}
						if (firstNotNull_startYear == null && startYear2 != null) {
							firstNotNull_startYear = startYear2;
						}
					}
					movie_res.setStartYear(firstNotNull_startYear);
					
					// attribute 'Movie.runtimeMinutes'
					Integer firstNotNull_runtimeMinutes = r.getAs("runtimeMinutes");
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer runtimeMinutes2 = r.getAs("runtimeMinutes_" + i);
						if (firstNotNull_runtimeMinutes != null && runtimeMinutes2 != null && !firstNotNull_runtimeMinutes.equals(runtimeMinutes2)) {
							movie_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Movie.runtimeMinutes': " + firstNotNull_runtimeMinutes + " and " + runtimeMinutes2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Movie.runtimeMinutes' ==> " + firstNotNull_runtimeMinutes + " and " + runtimeMinutes2);
						}
						if (firstNotNull_runtimeMinutes == null && runtimeMinutes2 != null) {
							firstNotNull_runtimeMinutes = runtimeMinutes2;
						}
					}
					movie_res.setRuntimeMinutes(firstNotNull_runtimeMinutes);
					
					// attribute 'Movie.averageRating'
					String firstNotNull_averageRating = r.getAs("averageRating");
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String averageRating2 = r.getAs("averageRating_" + i);
						if (firstNotNull_averageRating != null && averageRating2 != null && !firstNotNull_averageRating.equals(averageRating2)) {
							movie_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Movie.averageRating': " + firstNotNull_averageRating + " and " + averageRating2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Movie.averageRating' ==> " + firstNotNull_averageRating + " and " + averageRating2);
						}
						if (firstNotNull_averageRating == null && averageRating2 != null) {
							firstNotNull_averageRating = averageRating2;
						}
					}
					movie_res.setAverageRating(firstNotNull_averageRating);
					
					// attribute 'Movie.numVotes'
					Integer firstNotNull_numVotes = r.getAs("numVotes");
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer numVotes2 = r.getAs("numVotes_" + i);
						if (firstNotNull_numVotes != null && numVotes2 != null && !firstNotNull_numVotes.equals(numVotes2)) {
							movie_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Movie.numVotes': " + firstNotNull_numVotes + " and " + numVotes2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Movie.numVotes' ==> " + firstNotNull_numVotes + " and " + numVotes2);
						}
						if (firstNotNull_numVotes == null && numVotes2 != null) {
							firstNotNull_numVotes = numVotes2;
						}
					}
					movie_res.setNumVotes(firstNotNull_numVotes);
					return movie_res;
				}, Encoders.bean(Movie.class));
			return d;
	}
	
	
	public Dataset<Movie> getMovieList(Movie.movieDirector role, Director director) {
		if(role != null) {
			if(role.equals(Movie.movieDirector.directed_movie))
				return getDirected_movieListInMovieDirectorByDirector(director);
		}
		return null;
	}
	
	public Dataset<Movie> getMovieList(Movie.movieDirector role, Condition<DirectorAttribute> condition) {
		if(role != null) {
			if(role.equals(Movie.movieDirector.directed_movie))
				return getDirected_movieListInMovieDirectorByDirectorCondition(condition);
		}
		return null;
	}
	
	public Dataset<Movie> getMovieList(Movie.movieDirector role, Condition<MovieAttribute> condition1, Condition<DirectorAttribute> condition2) {
		if(role != null) {
			if(role.equals(Movie.movieDirector.directed_movie))
				return getDirected_movieListInMovieDirector(condition1, condition2);
		}
		return null;
	}
	
	
	
	public Dataset<Movie> getMovieList(Movie.movieActor role, Actor actor) {
		if(role != null) {
			if(role.equals(Movie.movieActor.movie))
				return getMovieListInMovieActorByCharacter(actor);
		}
		return null;
	}
	
	public Dataset<Movie> getMovieList(Movie.movieActor role, Condition<ActorAttribute> condition) {
		if(role != null) {
			if(role.equals(Movie.movieActor.movie))
				return getMovieListInMovieActorByCharacterCondition(condition);
		}
		return null;
	}
	
	public Dataset<Movie> getMovieList(Movie.movieActor role, Condition<ActorAttribute> condition1, Condition<MovieAttribute> condition2) {
		if(role != null) {
			if(role.equals(Movie.movieActor.movie))
				return getMovieListInMovieActor(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	public abstract Dataset<Movie> getDirected_movieListInMovieDirector(conditions.Condition<conditions.MovieAttribute> directed_movie_condition,conditions.Condition<conditions.DirectorAttribute> director_condition);
	
	public Dataset<Movie> getDirected_movieListInMovieDirectorByDirected_movieCondition(conditions.Condition<conditions.MovieAttribute> directed_movie_condition){
		return getDirected_movieListInMovieDirector(directed_movie_condition, null);
	}
	public Dataset<Movie> getDirected_movieListInMovieDirectorByDirectorCondition(conditions.Condition<conditions.DirectorAttribute> director_condition){
		return getDirected_movieListInMovieDirector(null, director_condition);
	}
	
	public Dataset<Movie> getDirected_movieListInMovieDirectorByDirector(pojo.Director director){
		if(director == null)
			return null;
	
		Condition c;
		c=Condition.simple(DirectorAttribute.id,Operator.EQUALS, director.getId());
		Dataset<Movie> res = getDirected_movieListInMovieDirectorByDirectorCondition(c);
		return res;
	}
	
	public abstract Dataset<Movie> getMovieListInMovieActor(conditions.Condition<conditions.ActorAttribute> character_condition,conditions.Condition<conditions.MovieAttribute> movie_condition);
	
	public Dataset<Movie> getMovieListInMovieActorByCharacterCondition(conditions.Condition<conditions.ActorAttribute> character_condition){
		return getMovieListInMovieActor(character_condition, null);
	}
	
	public Dataset<Movie> getMovieListInMovieActorByCharacter(pojo.Actor character){
		if(character == null)
			return null;
	
		Condition c;
		c=Condition.simple(ActorAttribute.id,Operator.EQUALS, character.getId());
		Dataset<Movie> res = getMovieListInMovieActorByCharacterCondition(c);
		return res;
	}
	
	public Dataset<Movie> getMovieListInMovieActorByMovieCondition(conditions.Condition<conditions.MovieAttribute> movie_condition){
		return getMovieListInMovieActor(null, movie_condition);
	}
	
	public abstract void insertMovieAndLinkedItems(Movie movie);
	public abstract void insertMovie(Movie movie);
	
	public abstract void insertMovieInMovieKVFromMyredis(Movie movie); public abstract void insertMovieInActorCollectionFromMymongo(Movie movie); 
	public abstract void updateMovieList(conditions.Condition<conditions.MovieAttribute> condition, conditions.SetClause<conditions.MovieAttribute> set);
	
	public void updateMovie(pojo.Movie movie) {
		//TODO using the id
		return;
	}
	public abstract void updateDirected_movieListInMovieDirector(
		conditions.Condition<conditions.MovieAttribute> directed_movie_condition,
		conditions.Condition<conditions.DirectorAttribute> director_condition,
		
		conditions.SetClause<conditions.MovieAttribute> set
	);
	
	public void updateDirected_movieListInMovieDirectorByDirected_movieCondition(
		conditions.Condition<conditions.MovieAttribute> directed_movie_condition,
		conditions.SetClause<conditions.MovieAttribute> set
	){
		updateDirected_movieListInMovieDirector(directed_movie_condition, null, set);
	}
	public void updateDirected_movieListInMovieDirectorByDirectorCondition(
		conditions.Condition<conditions.DirectorAttribute> director_condition,
		conditions.SetClause<conditions.MovieAttribute> set
	){
		updateDirected_movieListInMovieDirector(null, director_condition, set);
	}
	
	public void updateDirected_movieListInMovieDirectorByDirector(
		pojo.Director director,
		conditions.SetClause<conditions.MovieAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void updateMovieListInMovieActor(
		conditions.Condition<conditions.ActorAttribute> character_condition,
		conditions.Condition<conditions.MovieAttribute> movie_condition,
		
		conditions.SetClause<conditions.MovieAttribute> set
	);
	
	public void updateMovieListInMovieActorByCharacterCondition(
		conditions.Condition<conditions.ActorAttribute> character_condition,
		conditions.SetClause<conditions.MovieAttribute> set
	){
		updateMovieListInMovieActor(character_condition, null, set);
	}
	
	public void updateMovieListInMovieActorByCharacter(
		pojo.Actor character,
		conditions.SetClause<conditions.MovieAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateMovieListInMovieActorByMovieCondition(
		conditions.Condition<conditions.MovieAttribute> movie_condition,
		conditions.SetClause<conditions.MovieAttribute> set
	){
		updateMovieListInMovieActor(null, movie_condition, set);
	}
	
	
	public abstract void deleteMovieList(conditions.Condition<conditions.MovieAttribute> condition);
	
	public void deleteMovie(pojo.Movie movie) {
		//TODO using the id
		return;
	}
	public abstract void deleteDirected_movieListInMovieDirector(	
		conditions.Condition<conditions.MovieAttribute> directed_movie_condition,	
		conditions.Condition<conditions.DirectorAttribute> director_condition);
	
	public void deleteDirected_movieListInMovieDirectorByDirected_movieCondition(
		conditions.Condition<conditions.MovieAttribute> directed_movie_condition
	){
		deleteDirected_movieListInMovieDirector(directed_movie_condition, null);
	}
	public void deleteDirected_movieListInMovieDirectorByDirectorCondition(
		conditions.Condition<conditions.DirectorAttribute> director_condition
	){
		deleteDirected_movieListInMovieDirector(null, director_condition);
	}
	
	public void deleteDirected_movieListInMovieDirectorByDirector(
		pojo.Director director 
	){
		//TODO get id in condition
		return;	
	}
	
	public abstract void deleteMovieListInMovieActor(	
		conditions.Condition<conditions.ActorAttribute> character_condition,	
		conditions.Condition<conditions.MovieAttribute> movie_condition);
	
	public void deleteMovieListInMovieActorByCharacterCondition(
		conditions.Condition<conditions.ActorAttribute> character_condition
	){
		deleteMovieListInMovieActor(character_condition, null);
	}
	
	public void deleteMovieListInMovieActorByCharacter(
		pojo.Actor character 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteMovieListInMovieActorByMovieCondition(
		conditions.Condition<conditions.MovieAttribute> movie_condition
	){
		deleteMovieListInMovieActor(null, movie_condition);
	}
	
}
