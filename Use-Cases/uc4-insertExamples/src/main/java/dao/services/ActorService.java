package dao.services;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pojo.Actor;
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
import conditions.ActorAttribute;
import conditions.MovieAttribute;
import pojo.Movie;

public abstract class ActorService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ActorService.class);
	protected MovieActorService movieActorService = new dao.impl.MovieActorServiceImpl();
	


	public static enum ROLE_NAME {
		MOVIEACTOR_CHARACTER
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.MOVIEACTOR_CHARACTER, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public ActorService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public ActorService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<Actor> getActorList(conditions.Condition<conditions.ActorAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Actor>> datasets = new ArrayList<Dataset<Actor>>();
		Dataset<Actor> d = null;
		d = getActorListInReviewColFromMymongo(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		d = getActorListInMovieColFromMymongo(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		d = getActorListInActorCollectionFromMymongo(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsActor(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Actor>) r -> condition == null || condition.evaluate(r));
		d=d.dropDuplicates();
		return d;
	}
	
	
	
	
	public abstract Dataset<Actor> getActorListInReviewColFromMymongo(conditions.Condition<conditions.ActorAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	public abstract Dataset<Actor> getActorListInMovieColFromMymongo(conditions.Condition<conditions.ActorAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	public abstract Dataset<Actor> getActorListInActorCollectionFromMymongo(conditions.Condition<conditions.ActorAttribute> condition, MutableBoolean refilterFlag);
	
	
	// TODO get based on id(s). Ex:public Client getClientById(Long id)
	
	public Dataset<Actor> getActorListById(String id) {
		return getActorList(conditions.Condition.simple(conditions.ActorAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Actor> getActorListByFullName(String fullName) {
		return getActorList(conditions.Condition.simple(conditions.ActorAttribute.fullName, conditions.Operator.EQUALS, fullName));
	}
	
	public Dataset<Actor> getActorListByYearOfBirth(String yearOfBirth) {
		return getActorList(conditions.Condition.simple(conditions.ActorAttribute.yearOfBirth, conditions.Operator.EQUALS, yearOfBirth));
	}
	
	public Dataset<Actor> getActorListByYearOfDeath(String yearOfDeath) {
		return getActorList(conditions.Condition.simple(conditions.ActorAttribute.yearOfDeath, conditions.Operator.EQUALS, yearOfDeath));
	}
	
	
	
	protected static Dataset<Actor> fullOuterJoinsActor(List<Dataset<Actor>> datasetsPOJO) {
		return fullOuterJoinsActor(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Actor> fullLeftOuterJoinsActor(List<Dataset<Actor>> datasetsPOJO) {
		return fullOuterJoinsActor(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Actor> fullOuterJoinsActor(List<Dataset<Actor>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Actor> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("fullName", "fullName_1")
								.withColumnRenamed("yearOfBirth", "yearOfBirth_1")
								.withColumnRenamed("yearOfDeath", "yearOfDeath_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("fullName", "fullName_" + i)
								.withColumnRenamed("yearOfBirth", "yearOfBirth_" + i)
								.withColumnRenamed("yearOfDeath", "yearOfDeath_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			} 
			d = res.map((MapFunction<Row, Actor>) r -> {
					Actor actor_res = new Actor();
					
					// attribute 'Actor.id'
					String firstNotNull_id = Util.getStringValue(r.getAs("id"));
					actor_res.setId(firstNotNull_id);
					
					// attribute 'Actor.fullName'
					String firstNotNull_fullName = Util.getStringValue(r.getAs("fullName"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String fullName2 = Util.getStringValue(r.getAs("fullName_" + i));
						if (firstNotNull_fullName != null && fullName2 != null && !firstNotNull_fullName.equals(fullName2)) {
							actor_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Actor.fullName': " + firstNotNull_fullName + " and " + fullName2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Actor.fullName' ==> " + firstNotNull_fullName + " and " + fullName2);
						}
						if (firstNotNull_fullName == null && fullName2 != null) {
							firstNotNull_fullName = fullName2;
						}
					}
					actor_res.setFullName(firstNotNull_fullName);
					
					// attribute 'Actor.yearOfBirth'
					String firstNotNull_yearOfBirth = Util.getStringValue(r.getAs("yearOfBirth"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String yearOfBirth2 = Util.getStringValue(r.getAs("yearOfBirth_" + i));
						if (firstNotNull_yearOfBirth != null && yearOfBirth2 != null && !firstNotNull_yearOfBirth.equals(yearOfBirth2)) {
							actor_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Actor.yearOfBirth': " + firstNotNull_yearOfBirth + " and " + yearOfBirth2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Actor.yearOfBirth' ==> " + firstNotNull_yearOfBirth + " and " + yearOfBirth2);
						}
						if (firstNotNull_yearOfBirth == null && yearOfBirth2 != null) {
							firstNotNull_yearOfBirth = yearOfBirth2;
						}
					}
					actor_res.setYearOfBirth(firstNotNull_yearOfBirth);
					
					// attribute 'Actor.yearOfDeath'
					String firstNotNull_yearOfDeath = Util.getStringValue(r.getAs("yearOfDeath"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String yearOfDeath2 = Util.getStringValue(r.getAs("yearOfDeath_" + i));
						if (firstNotNull_yearOfDeath != null && yearOfDeath2 != null && !firstNotNull_yearOfDeath.equals(yearOfDeath2)) {
							actor_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Actor.yearOfDeath': " + firstNotNull_yearOfDeath + " and " + yearOfDeath2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Actor.yearOfDeath' ==> " + firstNotNull_yearOfDeath + " and " + yearOfDeath2);
						}
						if (firstNotNull_yearOfDeath == null && yearOfDeath2 != null) {
							firstNotNull_yearOfDeath = yearOfDeath2;
						}
					}
					actor_res.setYearOfDeath(firstNotNull_yearOfDeath);
	
					scala.collection.mutable.WrappedArray<String> logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							actor_res.addLogEvent(logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							actor_res.addLogEvent(logEvents.apply(j));
						}
					}
	
					return actor_res;
				}, Encoders.bean(Actor.class));
			return d;
	}
	
	
	
	public Dataset<Actor> getActorList(Actor.movieActor role, Movie movie) {
		if(role != null) {
			if(role.equals(Actor.movieActor.character))
				return getCharacterListInMovieActorByMovie(movie);
		}
		return null;
	}
	
	public Dataset<Actor> getActorList(Actor.movieActor role, Condition<MovieAttribute> condition) {
		if(role != null) {
			if(role.equals(Actor.movieActor.character))
				return getCharacterListInMovieActorByMovieCondition(condition);
		}
		return null;
	}
	
	public Dataset<Actor> getActorList(Actor.movieActor role, Condition<ActorAttribute> condition1, Condition<MovieAttribute> condition2) {
		if(role != null) {
			if(role.equals(Actor.movieActor.character))
				return getCharacterListInMovieActor(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	
	
	public abstract Dataset<Actor> getCharacterListInMovieActor(conditions.Condition<conditions.ActorAttribute> character_condition,conditions.Condition<conditions.MovieAttribute> movie_condition);
	
	public Dataset<Actor> getCharacterListInMovieActorByCharacterCondition(conditions.Condition<conditions.ActorAttribute> character_condition){
		return getCharacterListInMovieActor(character_condition, null);
	}
	public Dataset<Actor> getCharacterListInMovieActorByMovieCondition(conditions.Condition<conditions.MovieAttribute> movie_condition){
		return getCharacterListInMovieActor(null, movie_condition);
	}
	
	public Dataset<Actor> getCharacterListInMovieActorByMovie(pojo.Movie movie){
		if(movie == null)
			return null;
	
		Condition c;
		c=Condition.simple(MovieAttribute.id,Operator.EQUALS, movie.getId());
		Dataset<Actor> res = getCharacterListInMovieActorByMovieCondition(c);
		return res;
	}
	
	
	public abstract boolean insertActor(Actor actor);
	
	public abstract boolean insertActorInActorCollectionFromMymongo(Actor actor); 
	
	public abstract void updateActorList(conditions.Condition<conditions.ActorAttribute> condition, conditions.SetClause<conditions.ActorAttribute> set);
	
	public void updateActor(pojo.Actor actor) {
		//TODO using the id
		return;
	}
	public abstract void updateCharacterListInMovieActor(
		conditions.Condition<conditions.ActorAttribute> character_condition,
		conditions.Condition<conditions.MovieAttribute> movie_condition,
		
		conditions.SetClause<conditions.ActorAttribute> set
	);
	
	public void updateCharacterListInMovieActorByCharacterCondition(
		conditions.Condition<conditions.ActorAttribute> character_condition,
		conditions.SetClause<conditions.ActorAttribute> set
	){
		updateCharacterListInMovieActor(character_condition, null, set);
	}
	public void updateCharacterListInMovieActorByMovieCondition(
		conditions.Condition<conditions.MovieAttribute> movie_condition,
		conditions.SetClause<conditions.ActorAttribute> set
	){
		updateCharacterListInMovieActor(null, movie_condition, set);
	}
	
	public void updateCharacterListInMovieActorByMovie(
		pojo.Movie movie,
		conditions.SetClause<conditions.ActorAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	
	
	public abstract void deleteActorList(conditions.Condition<conditions.ActorAttribute> condition);
	
	public void deleteActor(pojo.Actor actor) {
		//TODO using the id
		return;
	}
	public abstract void deleteCharacterListInMovieActor(	
		conditions.Condition<conditions.ActorAttribute> character_condition,	
		conditions.Condition<conditions.MovieAttribute> movie_condition);
	
	public void deleteCharacterListInMovieActorByCharacterCondition(
		conditions.Condition<conditions.ActorAttribute> character_condition
	){
		deleteCharacterListInMovieActor(character_condition, null);
	}
	public void deleteCharacterListInMovieActorByMovieCondition(
		conditions.Condition<conditions.MovieAttribute> movie_condition
	){
		deleteCharacterListInMovieActor(null, movie_condition);
	}
	
	public void deleteCharacterListInMovieActorByMovie(
		pojo.Movie movie 
	){
		//TODO get id in condition
		return;	
	}
	
	
}
