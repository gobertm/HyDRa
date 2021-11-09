package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import pojo.Customer;
import java.time.LocalDate;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.commons.lang.mutable.MutableBoolean;
import conditions.Condition;
import conditions.Operator;
import util.Util;
import conditions.CustomerAttribute;
import conditions.OrderAttribute;
import pojo.Order;
import conditions.CustomerAttribute;
import conditions.FeedbackAttribute;
import pojo.Feedback;

public abstract class CustomerService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CustomerService.class);
	protected BuysService buysService = new dao.impl.BuysServiceImpl();
	protected WriteService writeService = new dao.impl.WriteServiceImpl();
	


	public static enum ROLE_NAME {
		BUYS_CLIENT, WRITE_REVIEWER
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.BUYS_CLIENT, loading.Loading.LAZY);
		defaultLoadingParameters.put(ROLE_NAME.WRITE_REVIEWER, loading.Loading.LAZY);
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public CustomerService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public CustomerService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
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
	
	
	public Dataset<Customer> getCustomerList(conditions.Condition<conditions.CustomerAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Customer>> datasets = new ArrayList<Dataset<Customer>>();
		Dataset<Customer> d = null;
		d = getCustomerListInCustomerTableFromMysqlbench(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
			d=fullOuterJoinsCustomer(datasets);
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Customer>) r -> condition == null || condition.evaluate(r));
		d = d.dropDuplicates(new String[] {"id"});
		return d;
	}
	
	
	
	
	public abstract Dataset<Customer> getCustomerListInCustomerTableFromMysqlbench(conditions.Condition<conditions.CustomerAttribute> condition, MutableBoolean refilterFlag);
	
	
	// TODO get based on id(s). Ex:public Client getClientById(Long id)
	public Customer getCustomerById(String id){
		Condition cond;
		cond = Condition.simple(CustomerAttribute.id, conditions.Operator.EQUALS, id);
		Dataset<Customer> res = getCustomerList(cond);
		if(res!=null)
			return res.first();
		return null;
	}
	
	public Dataset<Customer> getCustomerListById(String id) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Customer> getCustomerListByFirstname(String firstname) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.firstname, conditions.Operator.EQUALS, firstname));
	}
	
	public Dataset<Customer> getCustomerListByLastname(String lastname) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.lastname, conditions.Operator.EQUALS, lastname));
	}
	
	public Dataset<Customer> getCustomerListByGender(String gender) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.gender, conditions.Operator.EQUALS, gender));
	}
	
	public Dataset<Customer> getCustomerListByBirthday(LocalDate birthday) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.birthday, conditions.Operator.EQUALS, birthday));
	}
	
	public Dataset<Customer> getCustomerListByCreationDate(LocalDate creationDate) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.creationDate, conditions.Operator.EQUALS, creationDate));
	}
	
	public Dataset<Customer> getCustomerListByLocationip(String locationip) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.locationip, conditions.Operator.EQUALS, locationip));
	}
	
	public Dataset<Customer> getCustomerListByBrowser(String browser) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.browser, conditions.Operator.EQUALS, browser));
	}
	
	
	
	protected static Dataset<Customer> fullOuterJoinsCustomer(List<Dataset<Customer>> datasetsPOJO) {
		return fullOuterJoinsCustomer(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Customer> fullLeftOuterJoinsCustomer(List<Dataset<Customer>> datasetsPOJO) {
		return fullOuterJoinsCustomer(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Customer> fullOuterJoinsCustomer(List<Dataset<Customer>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Customer> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("firstname", "firstname_1")
								.withColumnRenamed("lastname", "lastname_1")
								.withColumnRenamed("gender", "gender_1")
								.withColumnRenamed("birthday", "birthday_1")
								.withColumnRenamed("creationDate", "creationDate_1")
								.withColumnRenamed("locationip", "locationip_1")
								.withColumnRenamed("browser", "browser_1")
								.withColumnRenamed("logEvents", "logEvents_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("firstname", "firstname_" + i)
								.withColumnRenamed("lastname", "lastname_" + i)
								.withColumnRenamed("gender", "gender_" + i)
								.withColumnRenamed("birthday", "birthday_" + i)
								.withColumnRenamed("creationDate", "creationDate_" + i)
								.withColumnRenamed("locationip", "locationip_" + i)
								.withColumnRenamed("browser", "browser_" + i)
								.withColumnRenamed("logEvents", "logEvents_" + i)
						, seq, joinMode);
			} 
			d = res.map((MapFunction<Row, Customer>) r -> {
					Customer customer_res = new Customer();
					
					// attribute 'Customer.id'
					String firstNotNull_id = Util.getStringValue(r.getAs("id"));
					customer_res.setId(firstNotNull_id);
					
					// attribute 'Customer.firstname'
					String firstNotNull_firstname = Util.getStringValue(r.getAs("firstname"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String firstname2 = Util.getStringValue(r.getAs("firstname_" + i));
						if (firstNotNull_firstname != null && firstname2 != null && !firstNotNull_firstname.equals(firstname2)) {
							customer_res.addLogEvent("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.firstname': " + firstNotNull_firstname + " and " + firstname2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.firstname': " + firstNotNull_firstname + " and " + firstname2 + "." );
						}
						if (firstNotNull_firstname == null && firstname2 != null) {
							firstNotNull_firstname = firstname2;
						}
					}
					customer_res.setFirstname(firstNotNull_firstname);
					
					// attribute 'Customer.lastname'
					String firstNotNull_lastname = Util.getStringValue(r.getAs("lastname"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String lastname2 = Util.getStringValue(r.getAs("lastname_" + i));
						if (firstNotNull_lastname != null && lastname2 != null && !firstNotNull_lastname.equals(lastname2)) {
							customer_res.addLogEvent("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.lastname': " + firstNotNull_lastname + " and " + lastname2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.lastname': " + firstNotNull_lastname + " and " + lastname2 + "." );
						}
						if (firstNotNull_lastname == null && lastname2 != null) {
							firstNotNull_lastname = lastname2;
						}
					}
					customer_res.setLastname(firstNotNull_lastname);
					
					// attribute 'Customer.gender'
					String firstNotNull_gender = Util.getStringValue(r.getAs("gender"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String gender2 = Util.getStringValue(r.getAs("gender_" + i));
						if (firstNotNull_gender != null && gender2 != null && !firstNotNull_gender.equals(gender2)) {
							customer_res.addLogEvent("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.gender': " + firstNotNull_gender + " and " + gender2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.gender': " + firstNotNull_gender + " and " + gender2 + "." );
						}
						if (firstNotNull_gender == null && gender2 != null) {
							firstNotNull_gender = gender2;
						}
					}
					customer_res.setGender(firstNotNull_gender);
					
					// attribute 'Customer.birthday'
					LocalDate firstNotNull_birthday = Util.getLocalDateValue(r.getAs("birthday"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate birthday2 = Util.getLocalDateValue(r.getAs("birthday_" + i));
						if (firstNotNull_birthday != null && birthday2 != null && !firstNotNull_birthday.equals(birthday2)) {
							customer_res.addLogEvent("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.birthday': " + firstNotNull_birthday + " and " + birthday2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.birthday': " + firstNotNull_birthday + " and " + birthday2 + "." );
						}
						if (firstNotNull_birthday == null && birthday2 != null) {
							firstNotNull_birthday = birthday2;
						}
					}
					customer_res.setBirthday(firstNotNull_birthday);
					
					// attribute 'Customer.creationDate'
					LocalDate firstNotNull_creationDate = Util.getLocalDateValue(r.getAs("creationDate"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						LocalDate creationDate2 = Util.getLocalDateValue(r.getAs("creationDate_" + i));
						if (firstNotNull_creationDate != null && creationDate2 != null && !firstNotNull_creationDate.equals(creationDate2)) {
							customer_res.addLogEvent("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.creationDate': " + firstNotNull_creationDate + " and " + creationDate2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.creationDate': " + firstNotNull_creationDate + " and " + creationDate2 + "." );
						}
						if (firstNotNull_creationDate == null && creationDate2 != null) {
							firstNotNull_creationDate = creationDate2;
						}
					}
					customer_res.setCreationDate(firstNotNull_creationDate);
					
					// attribute 'Customer.locationip'
					String firstNotNull_locationip = Util.getStringValue(r.getAs("locationip"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String locationip2 = Util.getStringValue(r.getAs("locationip_" + i));
						if (firstNotNull_locationip != null && locationip2 != null && !firstNotNull_locationip.equals(locationip2)) {
							customer_res.addLogEvent("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.locationip': " + firstNotNull_locationip + " and " + locationip2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.locationip': " + firstNotNull_locationip + " and " + locationip2 + "." );
						}
						if (firstNotNull_locationip == null && locationip2 != null) {
							firstNotNull_locationip = locationip2;
						}
					}
					customer_res.setLocationip(firstNotNull_locationip);
					
					// attribute 'Customer.browser'
					String firstNotNull_browser = Util.getStringValue(r.getAs("browser"));
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String browser2 = Util.getStringValue(r.getAs("browser_" + i));
						if (firstNotNull_browser != null && browser2 != null && !firstNotNull_browser.equals(browser2)) {
							customer_res.addLogEvent("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.browser': " + firstNotNull_browser + " and " + browser2 + "." );
							logger.warn("Data consistency problem for [Customer - id :"+customer_res.getId()+"]: different values found for attribute 'Customer.browser': " + firstNotNull_browser + " and " + browser2 + "." );
						}
						if (firstNotNull_browser == null && browser2 != null) {
							firstNotNull_browser = browser2;
						}
					}
					customer_res.setBrowser(firstNotNull_browser);
	
					WrappedArray logEvents = r.getAs("logEvents");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							customer_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							customer_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					return customer_res;
				}, Encoders.bean(Customer.class));
			return d;
	}
	
	
	public Customer getCustomer(Customer.buys role, Order order) {
		if(role != null) {
			if(role.equals(Customer.buys.client))
				return getClientInBuysByOrder(order);
		}
		return null;
	}
	
	public Dataset<Customer> getCustomerList(Customer.buys role, Condition<OrderAttribute> condition) {
		if(role != null) {
			if(role.equals(Customer.buys.client))
				return getClientListInBuysByOrderCondition(condition);
		}
		return null;
	}
	
	public Dataset<Customer> getCustomerList(Customer.buys role, Condition<OrderAttribute> condition1, Condition<CustomerAttribute> condition2) {
		if(role != null) {
			if(role.equals(Customer.buys.client))
				return getClientListInBuys(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	public Dataset<Customer> getCustomerList(Customer.write role, Feedback feedback) {
		if(role != null) {
			if(role.equals(Customer.write.reviewer))
				return getReviewerListInWriteByReview(feedback);
		}
		return null;
	}
	
	public Dataset<Customer> getCustomerList(Customer.write role, Condition<FeedbackAttribute> condition) {
		if(role != null) {
			if(role.equals(Customer.write.reviewer))
				return getReviewerListInWriteByReviewCondition(condition);
		}
		return null;
	}
	
	public Dataset<Customer> getCustomerList(Customer.write role, Condition<FeedbackAttribute> condition1, Condition<CustomerAttribute> condition2) {
		if(role != null) {
			if(role.equals(Customer.write.reviewer))
				return getReviewerListInWrite(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	
	public abstract Dataset<Customer> getClientListInBuys(conditions.Condition<conditions.OrderAttribute> order_condition,conditions.Condition<conditions.CustomerAttribute> client_condition);
	
	public Dataset<Customer> getClientListInBuysByOrderCondition(conditions.Condition<conditions.OrderAttribute> order_condition){
		return getClientListInBuys(order_condition, null);
	}
	
	public Customer getClientInBuysByOrder(pojo.Order order){
		if(order == null)
			return null;
	
		Condition c;
		c=Condition.simple(OrderAttribute.id,Operator.EQUALS, order.getId());
		Dataset<Customer> res = getClientListInBuysByOrderCondition(c);
		return !res.isEmpty()?res.first():null;
	}
	
	public Dataset<Customer> getClientListInBuysByClientCondition(conditions.Condition<conditions.CustomerAttribute> client_condition){
		return getClientListInBuys(null, client_condition);
	}
	public abstract Dataset<Customer> getReviewerListInWrite(conditions.Condition<conditions.FeedbackAttribute> review_condition,conditions.Condition<conditions.CustomerAttribute> reviewer_condition);
	
	public Dataset<Customer> getReviewerListInWriteByReviewCondition(conditions.Condition<conditions.FeedbackAttribute> review_condition){
		return getReviewerListInWrite(review_condition, null);
	}
	
	public Dataset<Customer> getReviewerListInWriteByReview(pojo.Feedback review){
		if(review == null)
			return null;
	
		Condition c;
		c=null;
		Dataset<Customer> res = getReviewerListInWriteByReviewCondition(c);
		return res;
	}
	
	public Dataset<Customer> getReviewerListInWriteByReviewerCondition(conditions.Condition<conditions.CustomerAttribute> reviewer_condition){
		return getReviewerListInWrite(null, reviewer_condition);
	}
	
	
	public abstract boolean insertCustomer(Customer customer);
	
	public abstract boolean insertCustomerInCustomerTableFromMysqlbench(Customer customer); 
	public abstract void updateCustomerList(conditions.Condition<conditions.CustomerAttribute> condition, conditions.SetClause<conditions.CustomerAttribute> set);
	
	public void updateCustomer(pojo.Customer customer) {
		//TODO using the id
		return;
	}
	public abstract void updateClientListInBuys(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.CustomerAttribute> client_condition,
		
		conditions.SetClause<conditions.CustomerAttribute> set
	);
	
	public void updateClientListInBuysByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.CustomerAttribute> set
	){
		updateClientListInBuys(order_condition, null, set);
	}
	
	public void updateClientInBuysByOrder(
		pojo.Order order,
		conditions.SetClause<conditions.CustomerAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateClientListInBuysByClientCondition(
		conditions.Condition<conditions.CustomerAttribute> client_condition,
		conditions.SetClause<conditions.CustomerAttribute> set
	){
		updateClientListInBuys(null, client_condition, set);
	}
	public abstract void updateReviewerListInWrite(
		conditions.Condition<conditions.FeedbackAttribute> review_condition,
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		
		conditions.SetClause<conditions.CustomerAttribute> set
	);
	
	public void updateReviewerListInWriteByReviewCondition(
		conditions.Condition<conditions.FeedbackAttribute> review_condition,
		conditions.SetClause<conditions.CustomerAttribute> set
	){
		updateReviewerListInWrite(review_condition, null, set);
	}
	
	public void updateReviewerListInWriteByReview(
		pojo.Feedback review,
		conditions.SetClause<conditions.CustomerAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	public void updateReviewerListInWriteByReviewerCondition(
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition,
		conditions.SetClause<conditions.CustomerAttribute> set
	){
		updateReviewerListInWrite(null, reviewer_condition, set);
	}
	
	
	public abstract void deleteCustomerList(conditions.Condition<conditions.CustomerAttribute> condition);
	
	public void deleteCustomer(pojo.Customer customer) {
		//TODO using the id
		return;
	}
	public abstract void deleteClientListInBuys(	
		conditions.Condition<conditions.OrderAttribute> order_condition,	
		conditions.Condition<conditions.CustomerAttribute> client_condition);
	
	public void deleteClientListInBuysByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteClientListInBuys(order_condition, null);
	}
	
	public void deleteClientInBuysByOrder(
		pojo.Order order 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteClientListInBuysByClientCondition(
		conditions.Condition<conditions.CustomerAttribute> client_condition
	){
		deleteClientListInBuys(null, client_condition);
	}
	public abstract void deleteReviewerListInWrite(	
		conditions.Condition<conditions.FeedbackAttribute> review_condition,	
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition);
	
	public void deleteReviewerListInWriteByReviewCondition(
		conditions.Condition<conditions.FeedbackAttribute> review_condition
	){
		deleteReviewerListInWrite(review_condition, null);
	}
	
	public void deleteReviewerListInWriteByReview(
		pojo.Feedback review 
	){
		//TODO get id in condition
		return;	
	}
	
	public void deleteReviewerListInWriteByReviewerCondition(
		conditions.Condition<conditions.CustomerAttribute> reviewer_condition
	){
		deleteReviewerListInWrite(null, reviewer_condition);
	}
	
}
