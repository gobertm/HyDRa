package dao.services;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pojo.Customer;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.commons.lang.mutable.MutableBoolean;
import conditions.Condition;
import conditions.Operator;
import conditions.CustomerAttribute;
import conditions.OrderAttribute;
import pojo.Order;

public abstract class CustomerService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CustomerService.class);
	protected PlacesService placesService = new dao.impl.PlacesServiceImpl();
	


	public static enum ROLE_NAME {
		PLACES_BUYER
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
		defaultLoadingParameters.put(ROLE_NAME.PLACES_BUYER, loading.Loading.LAZY);
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
		d = getCustomerListInClientCollectionFromMongo(condition, refilterFlag);
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
								.withColumnRenamed("address", "address_1")
							, seq, "fullouter");
			for(int i = 2; i < datasets.size(); i++) {
				res = res.join(datasets.get(i)
								.withColumnRenamed("firstName", "firstName_" + i)
								.withColumnRenamed("lastName", "lastName_" + i)
								.withColumnRenamed("address", "address_" + i)
							, seq, "fullouter");
			} 
			d = res.map((MapFunction<Row, Customer>) r -> {
					Customer customer_res = new Customer();
					
					// attribute 'Customer.id'
					Integer firstNotNull_id = r.getAs("id");
					customer_res.setId(firstNotNull_id);
					
					// attribute 'Customer.firstName'
					String firstNotNull_firstName = r.getAs("firstName");
					for (int i = 1; i < datasets.size(); i++) {
						String firstName2 = r.getAs("firstName_" + i);
						if (firstNotNull_firstName != null && firstName2 != null && !firstNotNull_firstName.equals(firstName2)) {
							customer_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Customer.firstName': " + firstNotNull_firstName + " and " + firstName2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Customer.firstName' ==> " + firstNotNull_firstName + " and " + firstName2);
						}
						if (firstNotNull_firstName == null && firstName2 != null) {
							firstNotNull_firstName = firstName2;
						}
					}
					customer_res.setFirstName(firstNotNull_firstName);
					
					// attribute 'Customer.lastName'
					String firstNotNull_lastName = r.getAs("lastName");
					for (int i = 1; i < datasets.size(); i++) {
						String lastName2 = r.getAs("lastName_" + i);
						if (firstNotNull_lastName != null && lastName2 != null && !firstNotNull_lastName.equals(lastName2)) {
							customer_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Customer.lastName': " + firstNotNull_lastName + " and " + lastName2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Customer.lastName' ==> " + firstNotNull_lastName + " and " + lastName2);
						}
						if (firstNotNull_lastName == null && lastName2 != null) {
							firstNotNull_lastName = lastName2;
						}
					}
					customer_res.setLastName(firstNotNull_lastName);
					
					// attribute 'Customer.address'
					String firstNotNull_address = r.getAs("address");
					for (int i = 1; i < datasets.size(); i++) {
						String address2 = r.getAs("address_" + i);
						if (firstNotNull_address != null && address2 != null && !firstNotNull_address.equals(address2)) {
							customer_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Customer.address': " + firstNotNull_address + " and " + address2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Customer.address' ==> " + firstNotNull_address + " and " + address2);
						}
						if (firstNotNull_address == null && address2 != null) {
							firstNotNull_address = address2;
						}
					}
					customer_res.setAddress(firstNotNull_address);
					return customer_res;
				}, Encoders.bean(Customer.class));
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Customer>) r -> condition == null || condition.evaluate(r));
		d=d.dropDuplicates();
		return d;
	}
	
	
	
	public abstract Dataset<Customer> getCustomerListInClientCollectionFromMongo(conditions.Condition<conditions.CustomerAttribute> condition, MutableBoolean refilterFlag);
	
	
	// TODO get based on id(s). Ex:public Client getClientById(Long id)
	
	public Dataset<Customer> getCustomerListById(Integer id) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Customer> getCustomerListByFirstName(String firstName) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.firstName, conditions.Operator.EQUALS, firstName));
	}
	
	public Dataset<Customer> getCustomerListByLastName(String lastName) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.lastName, conditions.Operator.EQUALS, lastName));
	}
	
	public Dataset<Customer> getCustomerListByAddress(String address) {
		return getCustomerList(conditions.Condition.simple(conditions.CustomerAttribute.address, conditions.Operator.EQUALS, address));
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
								.withColumnRenamed("firstName", "firstName_1")
								.withColumnRenamed("lastName", "lastName_1")
								.withColumnRenamed("address", "address_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("firstName", "firstName_" + i)
								.withColumnRenamed("lastName", "lastName_" + i)
								.withColumnRenamed("address", "address_" + i)
							, seq, joinMode);
			} 
			d = res.map((MapFunction<Row, Customer>) r -> {
					Customer customer_res = new Customer();
					
					// attribute 'Customer.id'
					Integer firstNotNull_id = r.getAs("id");
					customer_res.setId(firstNotNull_id);
					
					// attribute 'Customer.firstName'
					String firstNotNull_firstName = r.getAs("firstName");
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String firstName2 = r.getAs("firstName_" + i);
						if (firstNotNull_firstName != null && firstName2 != null && !firstNotNull_firstName.equals(firstName2)) {
							customer_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Customer.firstName': " + firstNotNull_firstName + " and " + firstName2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Customer.firstName' ==> " + firstNotNull_firstName + " and " + firstName2);
						}
						if (firstNotNull_firstName == null && firstName2 != null) {
							firstNotNull_firstName = firstName2;
						}
					}
					customer_res.setFirstName(firstNotNull_firstName);
					
					// attribute 'Customer.lastName'
					String firstNotNull_lastName = r.getAs("lastName");
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String lastName2 = r.getAs("lastName_" + i);
						if (firstNotNull_lastName != null && lastName2 != null && !firstNotNull_lastName.equals(lastName2)) {
							customer_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Customer.lastName': " + firstNotNull_lastName + " and " + lastName2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Customer.lastName' ==> " + firstNotNull_lastName + " and " + lastName2);
						}
						if (firstNotNull_lastName == null && lastName2 != null) {
							firstNotNull_lastName = lastName2;
						}
					}
					customer_res.setLastName(firstNotNull_lastName);
					
					// attribute 'Customer.address'
					String firstNotNull_address = r.getAs("address");
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String address2 = r.getAs("address_" + i);
						if (firstNotNull_address != null && address2 != null && !firstNotNull_address.equals(address2)) {
							customer_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Customer.address': " + firstNotNull_address + " and " + address2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Customer.address' ==> " + firstNotNull_address + " and " + address2);
						}
						if (firstNotNull_address == null && address2 != null) {
							firstNotNull_address = address2;
						}
					}
					customer_res.setAddress(firstNotNull_address);
					return customer_res;
				}, Encoders.bean(Customer.class));
			return d;
	}
	
	
	public Customer getCustomer(Customer.places role, Order order) {
		if(role != null) {
			if(role.equals(Customer.places.buyer))
				return getBuyerInPlacesByOrder(order);
		}
		return null;
	}
	
	public Dataset<Customer> getCustomerList(Customer.places role, Condition<OrderAttribute> condition) {
		if(role != null) {
			if(role.equals(Customer.places.buyer))
				return getBuyerListInPlacesByOrderCondition(condition);
		}
		return null;
	}
	
	public Dataset<Customer> getCustomerList(Customer.places role, Condition<CustomerAttribute> condition1, Condition<OrderAttribute> condition2) {
		if(role != null) {
			if(role.equals(Customer.places.buyer))
				return getBuyerListInPlaces(condition1, condition2);
		}
		return null;
	}
	
	
	
	
	
	
	
	public abstract Dataset<Customer> getBuyerListInPlaces(conditions.Condition<conditions.CustomerAttribute> buyer_condition,conditions.Condition<conditions.OrderAttribute> order_condition);
	
	public Dataset<Customer> getBuyerListInPlacesByBuyerCondition(conditions.Condition<conditions.CustomerAttribute> buyer_condition){
		return getBuyerListInPlaces(buyer_condition, null);
	}
	public Dataset<Customer> getBuyerListInPlacesByOrderCondition(conditions.Condition<conditions.OrderAttribute> order_condition){
		return getBuyerListInPlaces(null, order_condition);
	}
	
	public Customer getBuyerInPlacesByOrder(pojo.Order order){
		if(order == null)
			return null;
	
		Condition c;
		c=Condition.simple(OrderAttribute.id,Operator.EQUALS, order.getId());
		Dataset<Customer> res = getBuyerListInPlacesByOrderCondition(c);
		return res.first();
	}
	
	
	public abstract void insertCustomerAndLinkedItems(Customer customer);
	public abstract void insertCustomer(Customer customer);
	
	public abstract void insertCustomerInClientCollectionFromMongo(Customer customer); 
	public abstract void updateCustomerList(conditions.Condition<conditions.CustomerAttribute> condition, conditions.SetClause<conditions.CustomerAttribute> set);
	
	public void updateCustomer(pojo.Customer customer) {
		//TODO using the id
		return;
	}
	public abstract void updateBuyerListInPlaces(
		conditions.Condition<conditions.CustomerAttribute> buyer_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition,
		
		conditions.SetClause<conditions.CustomerAttribute> set
	);
	
	public void updateBuyerListInPlacesByBuyerCondition(
		conditions.Condition<conditions.CustomerAttribute> buyer_condition,
		conditions.SetClause<conditions.CustomerAttribute> set
	){
		updateBuyerListInPlaces(buyer_condition, null, set);
	}
	public void updateBuyerListInPlacesByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.SetClause<conditions.CustomerAttribute> set
	){
		updateBuyerListInPlaces(null, order_condition, set);
	}
	
	public void updateBuyerInPlacesByOrder(
		pojo.Order order,
		conditions.SetClause<conditions.CustomerAttribute> set 
	){
		//TODO get id in condition
		return;	
	}
	
	
	
	public abstract void deleteCustomerList(conditions.Condition<conditions.CustomerAttribute> condition);
	
	public void deleteCustomer(pojo.Customer customer) {
		//TODO using the id
		return;
	}
	public abstract void deleteBuyerListInPlaces(	
		conditions.Condition<conditions.CustomerAttribute> buyer_condition,	
		conditions.Condition<conditions.OrderAttribute> order_condition);
	
	public void deleteBuyerListInPlacesByBuyerCondition(
		conditions.Condition<conditions.CustomerAttribute> buyer_condition
	){
		deleteBuyerListInPlaces(buyer_condition, null);
	}
	public void deleteBuyerListInPlacesByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteBuyerListInPlaces(null, order_condition);
	}
	
	public void deleteBuyerInPlacesByOrder(
		pojo.Order order 
	){
		//TODO get id in condition
		return;	
	}
	
	
}
