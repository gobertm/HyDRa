package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.Condition;
import pojo.Feedback;
import java.time.LocalDate;
import tdo.ProductTDO;
import tdo.FeedbackTDO;
import pojo.Product;
import pojo.Feedback;
import conditions.ProductAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.MapFunction;
import util.Util;
import tdo.CustomerTDO;
import tdo.FeedbackTDO;
import pojo.Customer;
import pojo.Feedback;
import conditions.CustomerAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.MapFunction;
import util.Util;


public abstract class FeedbackService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FeedbackService.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	
	
	
	
	public static Dataset<Feedback> fullLeftOuterJoinBetweenFeedbackAndReviewedProduct(Dataset<Feedback> d1, Dataset<Product> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("id", "A_id")
			.withColumnRenamed("title", "A_title")
			.withColumnRenamed("price", "A_price")
			.withColumnRenamed("photo", "A_photo")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("reviewedProduct.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map(new MapFunction<Row, Feedback>() {
			public Feedback call(Row r) {
				Feedback res = new Feedback();
				res.setRate(r.getAs("rate"));
				res.setContent(r.getAs("content"));
	
				Product reviewedProduct = new Product();
				Object o = r.getAs("reviewedProduct");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						reviewedProduct.setId(Util.getStringValue(r2.getAs("id")));
						reviewedProduct.setTitle(Util.getStringValue(r2.getAs("title")));
						reviewedProduct.setPrice(Util.getDoubleValue(r2.getAs("price")));
						reviewedProduct.setPhoto(Util.getStringValue(r2.getAs("photo")));
					} 
					if(o instanceof Product) {
						reviewedProduct = (Product) o;
					}
				}
	
				res.setReviewedProduct(reviewedProduct);
	
				String id = Util.getStringValue(r.getAs("A_id"));
				if (reviewedProduct.getId() != null && id != null && !reviewedProduct.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Feedback - different values found for attribute 'Feedback.id': " + reviewedProduct.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Feedback - different values found for attribute 'Feedback.id': " + reviewedProduct.getId() + " and " + id + "." );
				}
				if(id != null)
					reviewedProduct.setId(id);
				String title = Util.getStringValue(r.getAs("A_title"));
				if (reviewedProduct.getTitle() != null && title != null && !reviewedProduct.getTitle().equals(title)) {
					res.addLogEvent("Data consistency problem for [Feedback - different values found for attribute 'Feedback.title': " + reviewedProduct.getTitle() + " and " + title + "." );
					logger.warn("Data consistency problem for [Feedback - different values found for attribute 'Feedback.title': " + reviewedProduct.getTitle() + " and " + title + "." );
				}
				if(title != null)
					reviewedProduct.setTitle(title);
				Double price = Util.getDoubleValue(r.getAs("A_price"));
				if (reviewedProduct.getPrice() != null && price != null && !reviewedProduct.getPrice().equals(price)) {
					res.addLogEvent("Data consistency problem for [Feedback - different values found for attribute 'Feedback.price': " + reviewedProduct.getPrice() + " and " + price + "." );
					logger.warn("Data consistency problem for [Feedback - different values found for attribute 'Feedback.price': " + reviewedProduct.getPrice() + " and " + price + "." );
				}
				if(price != null)
					reviewedProduct.setPrice(price);
				String photo = Util.getStringValue(r.getAs("A_photo"));
				if (reviewedProduct.getPhoto() != null && photo != null && !reviewedProduct.getPhoto().equals(photo)) {
					res.addLogEvent("Data consistency problem for [Feedback - different values found for attribute 'Feedback.photo': " + reviewedProduct.getPhoto() + " and " + photo + "." );
					logger.warn("Data consistency problem for [Feedback - different values found for attribute 'Feedback.photo': " + reviewedProduct.getPhoto() + " and " + photo + "." );
				}
				if(photo != null)
					reviewedProduct.setPhoto(photo);
	
				o = r.getAs("reviewer");
				Customer reviewer = new Customer();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						reviewer.setId(Util.getStringValue(r2.getAs("id")));
						reviewer.setFirstname(Util.getStringValue(r2.getAs("firstname")));
						reviewer.setLastname(Util.getStringValue(r2.getAs("lastname")));
						reviewer.setGender(Util.getStringValue(r2.getAs("gender")));
						reviewer.setBirthday(Util.getLocalDateValue(r2.getAs("birthday")));
						reviewer.setCreationDate(Util.getLocalDateValue(r2.getAs("creationDate")));
						reviewer.setLocationip(Util.getStringValue(r2.getAs("locationip")));
						reviewer.setBrowser(Util.getStringValue(r2.getAs("browser")));
					} 
					if(o instanceof Customer) {
						reviewer = (Customer) o;
					}
				}
	
				res.setReviewer(reviewer);
	
				return res;
			}
					
		}, Encoders.bean(Feedback.class));
	
		
		
	}
	public static Dataset<Feedback> fullLeftOuterJoinBetweenFeedbackAndReviewer(Dataset<Feedback> d1, Dataset<Customer> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("id", "A_id")
			.withColumnRenamed("firstname", "A_firstname")
			.withColumnRenamed("lastname", "A_lastname")
			.withColumnRenamed("gender", "A_gender")
			.withColumnRenamed("birthday", "A_birthday")
			.withColumnRenamed("creationDate", "A_creationDate")
			.withColumnRenamed("locationip", "A_locationip")
			.withColumnRenamed("browser", "A_browser")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("reviewer.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map(new MapFunction<Row, Feedback>() {
			public Feedback call(Row r) {
				Feedback res = new Feedback();
				res.setRate(r.getAs("rate"));
				res.setContent(r.getAs("content"));
	
				Customer reviewer = new Customer();
				Object o = r.getAs("reviewer");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						reviewer.setId(Util.getStringValue(r2.getAs("id")));
						reviewer.setFirstname(Util.getStringValue(r2.getAs("firstname")));
						reviewer.setLastname(Util.getStringValue(r2.getAs("lastname")));
						reviewer.setGender(Util.getStringValue(r2.getAs("gender")));
						reviewer.setBirthday(Util.getLocalDateValue(r2.getAs("birthday")));
						reviewer.setCreationDate(Util.getLocalDateValue(r2.getAs("creationDate")));
						reviewer.setLocationip(Util.getStringValue(r2.getAs("locationip")));
						reviewer.setBrowser(Util.getStringValue(r2.getAs("browser")));
					} 
					if(o instanceof Customer) {
						reviewer = (Customer) o;
					}
				}
	
				res.setReviewer(reviewer);
	
				String id = Util.getStringValue(r.getAs("A_id"));
				if (reviewer.getId() != null && id != null && !reviewer.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Feedback - different values found for attribute 'Feedback.id': " + reviewer.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Feedback - different values found for attribute 'Feedback.id': " + reviewer.getId() + " and " + id + "." );
				}
				if(id != null)
					reviewer.setId(id);
				String firstname = Util.getStringValue(r.getAs("A_firstname"));
				if (reviewer.getFirstname() != null && firstname != null && !reviewer.getFirstname().equals(firstname)) {
					res.addLogEvent("Data consistency problem for [Feedback - different values found for attribute 'Feedback.firstname': " + reviewer.getFirstname() + " and " + firstname + "." );
					logger.warn("Data consistency problem for [Feedback - different values found for attribute 'Feedback.firstname': " + reviewer.getFirstname() + " and " + firstname + "." );
				}
				if(firstname != null)
					reviewer.setFirstname(firstname);
				String lastname = Util.getStringValue(r.getAs("A_lastname"));
				if (reviewer.getLastname() != null && lastname != null && !reviewer.getLastname().equals(lastname)) {
					res.addLogEvent("Data consistency problem for [Feedback - different values found for attribute 'Feedback.lastname': " + reviewer.getLastname() + " and " + lastname + "." );
					logger.warn("Data consistency problem for [Feedback - different values found for attribute 'Feedback.lastname': " + reviewer.getLastname() + " and " + lastname + "." );
				}
				if(lastname != null)
					reviewer.setLastname(lastname);
				String gender = Util.getStringValue(r.getAs("A_gender"));
				if (reviewer.getGender() != null && gender != null && !reviewer.getGender().equals(gender)) {
					res.addLogEvent("Data consistency problem for [Feedback - different values found for attribute 'Feedback.gender': " + reviewer.getGender() + " and " + gender + "." );
					logger.warn("Data consistency problem for [Feedback - different values found for attribute 'Feedback.gender': " + reviewer.getGender() + " and " + gender + "." );
				}
				if(gender != null)
					reviewer.setGender(gender);
				LocalDate birthday = Util.getLocalDateValue(r.getAs("A_birthday"));
				if (reviewer.getBirthday() != null && birthday != null && !reviewer.getBirthday().equals(birthday)) {
					res.addLogEvent("Data consistency problem for [Feedback - different values found for attribute 'Feedback.birthday': " + reviewer.getBirthday() + " and " + birthday + "." );
					logger.warn("Data consistency problem for [Feedback - different values found for attribute 'Feedback.birthday': " + reviewer.getBirthday() + " and " + birthday + "." );
				}
				if(birthday != null)
					reviewer.setBirthday(birthday);
				LocalDate creationDate = Util.getLocalDateValue(r.getAs("A_creationDate"));
				if (reviewer.getCreationDate() != null && creationDate != null && !reviewer.getCreationDate().equals(creationDate)) {
					res.addLogEvent("Data consistency problem for [Feedback - different values found for attribute 'Feedback.creationDate': " + reviewer.getCreationDate() + " and " + creationDate + "." );
					logger.warn("Data consistency problem for [Feedback - different values found for attribute 'Feedback.creationDate': " + reviewer.getCreationDate() + " and " + creationDate + "." );
				}
				if(creationDate != null)
					reviewer.setCreationDate(creationDate);
				String locationip = Util.getStringValue(r.getAs("A_locationip"));
				if (reviewer.getLocationip() != null && locationip != null && !reviewer.getLocationip().equals(locationip)) {
					res.addLogEvent("Data consistency problem for [Feedback - different values found for attribute 'Feedback.locationip': " + reviewer.getLocationip() + " and " + locationip + "." );
					logger.warn("Data consistency problem for [Feedback - different values found for attribute 'Feedback.locationip': " + reviewer.getLocationip() + " and " + locationip + "." );
				}
				if(locationip != null)
					reviewer.setLocationip(locationip);
				String browser = Util.getStringValue(r.getAs("A_browser"));
				if (reviewer.getBrowser() != null && browser != null && !reviewer.getBrowser().equals(browser)) {
					res.addLogEvent("Data consistency problem for [Feedback - different values found for attribute 'Feedback.browser': " + reviewer.getBrowser() + " and " + browser + "." );
					logger.warn("Data consistency problem for [Feedback - different values found for attribute 'Feedback.browser': " + reviewer.getBrowser() + " and " + browser + "." );
				}
				if(browser != null)
					reviewer.setBrowser(browser);
	
				o = r.getAs("reviewedProduct");
				Product reviewedProduct = new Product();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						reviewedProduct.setId(Util.getStringValue(r2.getAs("id")));
						reviewedProduct.setTitle(Util.getStringValue(r2.getAs("title")));
						reviewedProduct.setPrice(Util.getDoubleValue(r2.getAs("price")));
						reviewedProduct.setPhoto(Util.getStringValue(r2.getAs("photo")));
					} 
					if(o instanceof Product) {
						reviewedProduct = (Product) o;
					}
				}
	
				res.setReviewedProduct(reviewedProduct);
	
				return res;
			}
					
		}, Encoders.bean(Feedback.class));
	
		
		
	}
	
	public static Dataset<Feedback> fullOuterJoinsFeedback(List<Dataset<Feedback>> datasetsPOJO) {
		return fullOuterJoinsFeedback(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Feedback> fullLeftOuterJoinsFeedback(List<Dataset<Feedback>> datasetsPOJO) {
		return fullOuterJoinsFeedback(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Feedback> fullOuterJoinsFeedback(List<Dataset<Feedback>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("reviewedProduct.id");
	
		idFields.add("reviewer.id");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<Feedback> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("reviewedProduct_id_" + i, d.col("reviewedProduct.id"))
				.withColumn("reviewer_id_" + i, d.col("reviewer.id"))
				.withColumnRenamed("rate", "rate_" + i)
				.withColumnRenamed("content", "content_" + i)
				.withColumnRenamed("reviewedProduct", "reviewedProduct_" + i)
				.withColumnRenamed("reviewer", "reviewer_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("reviewedProduct_id_0").equalTo(rows.get(1).col("reviewedProduct_id_1"));
		joinCond = joinCond.and(rows.get(0).col("reviewer_id_0").equalTo(rows.get(1).col("reviewer_id_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("reviewedProduct_id_" + (i - 1)).equalTo(rows.get(i).col("reviewedProduct_id_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("reviewer_id_" + (i - 1)).equalTo(rows.get(i).col("reviewer_id_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map(new MapFunction<Row, Feedback>() {
			public Feedback call(Row r) {
				Feedback feedback_res = new Feedback();
					
					// attribute 'Feedback.rate'
					Double firstNotNull_rate = Util.getDoubleValue(r.getAs("rate_0"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Double rate2 = Util.getDoubleValue(r.getAs("rate_" + i));
						if (firstNotNull_rate != null && rate2 != null && !firstNotNull_rate.equals(rate2)) {
							feedback_res.addLogEvent("Data consistency problem for [Feedback - different values found for attribute 'Feedback.rate': " + firstNotNull_rate + " and " + rate2 + "." );
							logger.warn("Data consistency problem for [Feedback - different values found for attribute 'Feedback.rate': " + firstNotNull_rate + " and " + rate2 + "." );
						}
						if (firstNotNull_rate == null && rate2 != null) {
							firstNotNull_rate = rate2;
						}
					}
					feedback_res.setRate(firstNotNull_rate);
					
					// attribute 'Feedback.content'
					String firstNotNull_content = Util.getStringValue(r.getAs("content_0"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String content2 = Util.getStringValue(r.getAs("content_" + i));
						if (firstNotNull_content != null && content2 != null && !firstNotNull_content.equals(content2)) {
							feedback_res.addLogEvent("Data consistency problem for [Feedback - different values found for attribute 'Feedback.content': " + firstNotNull_content + " and " + content2 + "." );
							logger.warn("Data consistency problem for [Feedback - different values found for attribute 'Feedback.content': " + firstNotNull_content + " and " + content2 + "." );
						}
						if (firstNotNull_content == null && content2 != null) {
							firstNotNull_content = content2;
						}
					}
					feedback_res.setContent(firstNotNull_content);
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							feedback_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							feedback_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Product reviewedProduct = new Product();
					reviewedProduct.setId(Util.getStringValue(r.getAs("reviewedProduct_0.id")));
					reviewedProduct.setTitle(Util.getStringValue(r.getAs("reviewedProduct_0.title")));
					reviewedProduct.setPrice(Util.getDoubleValue(r.getAs("reviewedProduct_0.price")));
					reviewedProduct.setPhoto(Util.getStringValue(r.getAs("reviewedProduct_0.photo")));
	
					Customer reviewer = new Customer();
					reviewer.setId(Util.getStringValue(r.getAs("reviewer_0.id")));
					reviewer.setFirstname(Util.getStringValue(r.getAs("reviewer_0.firstname")));
					reviewer.setLastname(Util.getStringValue(r.getAs("reviewer_0.lastname")));
					reviewer.setGender(Util.getStringValue(r.getAs("reviewer_0.gender")));
					reviewer.setBirthday(Util.getLocalDateValue(r.getAs("reviewer_0.birthday")));
					reviewer.setCreationDate(Util.getLocalDateValue(r.getAs("reviewer_0.creationDate")));
					reviewer.setLocationip(Util.getStringValue(r.getAs("reviewer_0.locationip")));
					reviewer.setBrowser(Util.getStringValue(r.getAs("reviewer_0.browser")));
	
					feedback_res.setReviewedProduct(reviewedProduct);
					feedback_res.setReviewer(reviewer);
					return feedback_res;
			}
		}
		, Encoders.bean(Feedback.class));
	
	}
	
	
	
	
	public abstract Dataset<pojo.Feedback> getFeedbackList(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		conditions.Condition<conditions.FeedbackAttribute> feedback_condition
	);
	
	public Dataset<pojo.Feedback> getFeedbackListByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition
	){
		return getFeedbackList(reviewedProduct_condition, null, null);
	}
	
	public Dataset<pojo.Feedback> getFeedbackListByReviewedProduct(pojo.Product reviewedProduct) {
		conditions.Condition<conditions.ProductAttribute> cond = null;
		cond = conditions.Condition.simple(conditions.ProductAttribute.id, conditions.Operator.EQUALS, reviewedProduct.getId());
		Dataset<pojo.Feedback> res = getFeedbackListByReviewedProductCondition(cond);
	return res;
	}
	public Dataset<pojo.Feedback> getFeedbackListByReviewerCondition(
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition
	){
		return getFeedbackList(null, reviewer_condition, null);
	}
	
	public Dataset<pojo.Feedback> getFeedbackListByReviewer(pojo.Customer reviewer) {
		conditions.Condition<conditions.CustomerAttribute> cond = null;
		cond = conditions.Condition.simple(conditions.CustomerAttribute.id, conditions.Operator.EQUALS, reviewer.getId());
		Dataset<pojo.Feedback> res = getFeedbackListByReviewerCondition(cond);
	return res;
	}
	
	public Dataset<pojo.Feedback> getFeedbackListByFeedbackCondition(
		conditions.Condition<conditions.FeedbackAttribute> feedback_condition
	){
		return getFeedbackList(null, null, feedback_condition);
	}
	
	public abstract void insertFeedback(Feedback feedback);
	
	
	
	
	 public void insertFeedback(Product reviewedProduct ,Customer reviewer ){
		Feedback feedback = new Feedback();
		feedback.setReviewedProduct(reviewedProduct);
		feedback.setReviewer(reviewer);
		insertFeedback(feedback);
	}
	
	 public void insertFeedback(Customer customer, List<Product> reviewedProductList){
		Feedback feedback = new Feedback();
		feedback.setReviewer(customer);
		for(Product reviewedProduct : reviewedProductList){
			feedback.setReviewedProduct(reviewedProduct);
			insertFeedback(feedback);
		}
	}
	 public void insertFeedback(Product product, List<Customer> reviewerList){
		Feedback feedback = new Feedback();
		feedback.setReviewedProduct(product);
		for(Customer reviewer : reviewerList){
			feedback.setReviewer(reviewer);
			insertFeedback(feedback);
		}
	}
	
	public abstract void updateFeedbackList(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		conditions.Condition<conditions.FeedbackAttribute> feedback_condition,
		conditions.SetClause<conditions.FeedbackAttribute> set
	);
	
	public void updateFeedbackListByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,
		conditions.SetClause<conditions.FeedbackAttribute> set
	){
		updateFeedbackList(reviewedProduct_condition, null, null, set);
	}
	
	public void updateFeedbackListByReviewedProduct(pojo.Product reviewedProduct, conditions.SetClause<conditions.FeedbackAttribute> set) {
		// TODO using id for selecting
		return;
	}
	public void updateFeedbackListByReviewerCondition(
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		conditions.SetClause<conditions.FeedbackAttribute> set
	){
		updateFeedbackList(null, reviewer_condition, null, set);
	}
	
	public void updateFeedbackListByReviewer(pojo.Customer reviewer, conditions.SetClause<conditions.FeedbackAttribute> set) {
		// TODO using id for selecting
		return;
	}
	
	public void updateFeedbackListByFeedbackCondition(
		conditions.Condition<conditions.FeedbackAttribute> feedback_condition,
		conditions.SetClause<conditions.FeedbackAttribute> set
	){
		updateFeedbackList(null, null, feedback_condition, set);
	}
	
	public abstract void deleteFeedbackList(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition,
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		conditions.Condition<conditions.FeedbackAttribute> feedback_condition);
	
	public void deleteFeedbackListByReviewedProductCondition(
		conditions.Condition<conditions.ProductAttribute> reviewedProduct_condition
	){
		deleteFeedbackList(reviewedProduct_condition, null, null);
	}
	
	public void deleteFeedbackListByReviewedProduct(pojo.Product reviewedProduct) {
		// TODO using id for selecting
		return;
	}
	public void deleteFeedbackListByReviewerCondition(
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition
	){
		deleteFeedbackList(null, reviewer_condition, null);
	}
	
	public void deleteFeedbackListByReviewer(pojo.Customer reviewer) {
		// TODO using id for selecting
		return;
	}
	
	public void deleteFeedbackListByFeedbackCondition(
		conditions.Condition<conditions.FeedbackAttribute> feedback_condition
	){
		deleteFeedbackList(null, null, feedback_condition);
	}
		
}
