package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.Condition;
import pojo.Buys;
import java.time.LocalDate;
import tdo.OrderTDO;
import tdo.BuysTDO;
import pojo.Order;
import pojo.Buys;
import conditions.OrderAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.MapFunction;
import util.Util;
import tdo.CustomerTDO;
import tdo.BuysTDO;
import pojo.Customer;
import pojo.Buys;
import conditions.CustomerAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.MapFunction;
import util.Util;


public abstract class BuysService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BuysService.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	
	
	// Left side 'customerId' of reference [clientRef ]
	public abstract Dataset<OrderTDO> getOrderTDOListOrderInClientRefInOrderTableFromRelSchemaB(Condition<OrderAttribute> condition, MutableBoolean refilterFlag);
	
	// Right side 'id' of reference [clientRef ]
	public abstract Dataset<CustomerTDO> getCustomerTDOListClientInClientRefInOrderTableFromRelSchemaB(Condition<CustomerAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	// method accessing the embedded object orders mapped to role client
	public abstract Dataset<pojo.Buys> getBuysListInmongoSchemaBuserColorders(Condition<CustomerAttribute> client_condition, Condition<OrderAttribute> order_condition, MutableBoolean client_refilter, MutableBoolean order_refilter);
	
	
	public static Dataset<Buys> fullLeftOuterJoinBetweenBuysAndOrder(Dataset<Buys> d1, Dataset<Order> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("id", "A_id")
			.withColumnRenamed("orderdate", "A_orderdate")
			.withColumnRenamed("totalprice", "A_totalprice")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("order.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map(new MapFunction<Row, Buys>() {
			public Buys call(Row r) {
				Buys res = new Buys();
	
				Order order = new Order();
				Object o = r.getAs("order");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						order.setId(Util.getStringValue(r2.getAs("id")));
						order.setOrderdate(Util.getLocalDateValue(r2.getAs("orderdate")));
						order.setTotalprice(Util.getDoubleValue(r2.getAs("totalprice")));
					} 
					if(o instanceof Order) {
						order = (Order) o;
					}
				}
	
				res.setOrder(order);
	
				String id = Util.getStringValue(r.getAs("A_id"));
				if (order.getId() != null && id != null && !order.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.id': " + order.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.id': " + order.getId() + " and " + id + "." );
				}
				if(id != null)
					order.setId(id);
				LocalDate orderdate = Util.getLocalDateValue(r.getAs("A_orderdate"));
				if (order.getOrderdate() != null && orderdate != null && !order.getOrderdate().equals(orderdate)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.orderdate': " + order.getOrderdate() + " and " + orderdate + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.orderdate': " + order.getOrderdate() + " and " + orderdate + "." );
				}
				if(orderdate != null)
					order.setOrderdate(orderdate);
				Double totalprice = Util.getDoubleValue(r.getAs("A_totalprice"));
				if (order.getTotalprice() != null && totalprice != null && !order.getTotalprice().equals(totalprice)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.totalprice': " + order.getTotalprice() + " and " + totalprice + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.totalprice': " + order.getTotalprice() + " and " + totalprice + "." );
				}
				if(totalprice != null)
					order.setTotalprice(totalprice);
	
				o = r.getAs("client");
				Customer client = new Customer();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						client.setId(Util.getStringValue(r2.getAs("id")));
						client.setFirstname(Util.getStringValue(r2.getAs("firstname")));
						client.setLastname(Util.getStringValue(r2.getAs("lastname")));
						client.setGender(Util.getStringValue(r2.getAs("gender")));
						client.setBirthday(Util.getLocalDateValue(r2.getAs("birthday")));
						client.setCreationDate(Util.getLocalDateValue(r2.getAs("creationDate")));
						client.setLocationip(Util.getStringValue(r2.getAs("locationip")));
						client.setBrowser(Util.getStringValue(r2.getAs("browser")));
					} 
					if(o instanceof Customer) {
						client = (Customer) o;
					}
				}
	
				res.setClient(client);
	
				return res;
			}
					
		}, Encoders.bean(Buys.class));
	
		
		
	}
	public static Dataset<Buys> fullLeftOuterJoinBetweenBuysAndClient(Dataset<Buys> d1, Dataset<Customer> d2) {
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
		joinCond = d1.col("client.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map(new MapFunction<Row, Buys>() {
			public Buys call(Row r) {
				Buys res = new Buys();
	
				Customer client = new Customer();
				Object o = r.getAs("client");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						client.setId(Util.getStringValue(r2.getAs("id")));
						client.setFirstname(Util.getStringValue(r2.getAs("firstname")));
						client.setLastname(Util.getStringValue(r2.getAs("lastname")));
						client.setGender(Util.getStringValue(r2.getAs("gender")));
						client.setBirthday(Util.getLocalDateValue(r2.getAs("birthday")));
						client.setCreationDate(Util.getLocalDateValue(r2.getAs("creationDate")));
						client.setLocationip(Util.getStringValue(r2.getAs("locationip")));
						client.setBrowser(Util.getStringValue(r2.getAs("browser")));
					} 
					if(o instanceof Customer) {
						client = (Customer) o;
					}
				}
	
				res.setClient(client);
	
				String id = Util.getStringValue(r.getAs("A_id"));
				if (client.getId() != null && id != null && !client.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.id': " + client.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.id': " + client.getId() + " and " + id + "." );
				}
				if(id != null)
					client.setId(id);
				String firstname = Util.getStringValue(r.getAs("A_firstname"));
				if (client.getFirstname() != null && firstname != null && !client.getFirstname().equals(firstname)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.firstname': " + client.getFirstname() + " and " + firstname + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.firstname': " + client.getFirstname() + " and " + firstname + "." );
				}
				if(firstname != null)
					client.setFirstname(firstname);
				String lastname = Util.getStringValue(r.getAs("A_lastname"));
				if (client.getLastname() != null && lastname != null && !client.getLastname().equals(lastname)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.lastname': " + client.getLastname() + " and " + lastname + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.lastname': " + client.getLastname() + " and " + lastname + "." );
				}
				if(lastname != null)
					client.setLastname(lastname);
				String gender = Util.getStringValue(r.getAs("A_gender"));
				if (client.getGender() != null && gender != null && !client.getGender().equals(gender)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.gender': " + client.getGender() + " and " + gender + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.gender': " + client.getGender() + " and " + gender + "." );
				}
				if(gender != null)
					client.setGender(gender);
				LocalDate birthday = Util.getLocalDateValue(r.getAs("A_birthday"));
				if (client.getBirthday() != null && birthday != null && !client.getBirthday().equals(birthday)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.birthday': " + client.getBirthday() + " and " + birthday + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.birthday': " + client.getBirthday() + " and " + birthday + "." );
				}
				if(birthday != null)
					client.setBirthday(birthday);
				LocalDate creationDate = Util.getLocalDateValue(r.getAs("A_creationDate"));
				if (client.getCreationDate() != null && creationDate != null && !client.getCreationDate().equals(creationDate)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.creationDate': " + client.getCreationDate() + " and " + creationDate + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.creationDate': " + client.getCreationDate() + " and " + creationDate + "." );
				}
				if(creationDate != null)
					client.setCreationDate(creationDate);
				String locationip = Util.getStringValue(r.getAs("A_locationip"));
				if (client.getLocationip() != null && locationip != null && !client.getLocationip().equals(locationip)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.locationip': " + client.getLocationip() + " and " + locationip + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.locationip': " + client.getLocationip() + " and " + locationip + "." );
				}
				if(locationip != null)
					client.setLocationip(locationip);
				String browser = Util.getStringValue(r.getAs("A_browser"));
				if (client.getBrowser() != null && browser != null && !client.getBrowser().equals(browser)) {
					res.addLogEvent("Data consistency problem for [Buys - different values found for attribute 'Buys.browser': " + client.getBrowser() + " and " + browser + "." );
					logger.warn("Data consistency problem for [Buys - different values found for attribute 'Buys.browser': " + client.getBrowser() + " and " + browser + "." );
				}
				if(browser != null)
					client.setBrowser(browser);
	
				o = r.getAs("order");
				Order order = new Order();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						order.setId(Util.getStringValue(r2.getAs("id")));
						order.setOrderdate(Util.getLocalDateValue(r2.getAs("orderdate")));
						order.setTotalprice(Util.getDoubleValue(r2.getAs("totalprice")));
					} 
					if(o instanceof Order) {
						order = (Order) o;
					}
				}
	
				res.setOrder(order);
	
				return res;
			}
					
		}, Encoders.bean(Buys.class));
	
		
		
	}
	
	public static Dataset<Buys> fullOuterJoinsBuys(List<Dataset<Buys>> datasetsPOJO) {
		return fullOuterJoinsBuys(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Buys> fullLeftOuterJoinsBuys(List<Dataset<Buys>> datasetsPOJO) {
		return fullOuterJoinsBuys(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Buys> fullOuterJoinsBuys(List<Dataset<Buys>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("order.id");
	
		idFields.add("client.id");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<Buys> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("order_id_" + i, d.col("order.id"))
				.withColumn("client_id_" + i, d.col("client.id"))
				.withColumnRenamed("order", "order_" + i)
				.withColumnRenamed("client", "client_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("order_id_0").equalTo(rows.get(1).col("order_id_1"));
		joinCond = joinCond.and(rows.get(0).col("client_id_0").equalTo(rows.get(1).col("client_id_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("order_id_" + (i - 1)).equalTo(rows.get(i).col("order_id_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("client_id_" + (i - 1)).equalTo(rows.get(i).col("client_id_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map(new MapFunction<Row, Buys>() {
			public Buys call(Row r) {
				Buys buys_res = new Buys();
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							buys_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							buys_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Order order = new Order();
					order.setId(Util.getStringValue(r.getAs("order_0.id")));
					order.setOrderdate(Util.getLocalDateValue(r.getAs("order_0.orderdate")));
					order.setTotalprice(Util.getDoubleValue(r.getAs("order_0.totalprice")));
	
					Customer client = new Customer();
					client.setId(Util.getStringValue(r.getAs("client_0.id")));
					client.setFirstname(Util.getStringValue(r.getAs("client_0.firstname")));
					client.setLastname(Util.getStringValue(r.getAs("client_0.lastname")));
					client.setGender(Util.getStringValue(r.getAs("client_0.gender")));
					client.setBirthday(Util.getLocalDateValue(r.getAs("client_0.birthday")));
					client.setCreationDate(Util.getLocalDateValue(r.getAs("client_0.creationDate")));
					client.setLocationip(Util.getStringValue(r.getAs("client_0.locationip")));
					client.setBrowser(Util.getStringValue(r.getAs("client_0.browser")));
	
					buys_res.setOrder(order);
					buys_res.setClient(client);
					return buys_res;
			}
		}
		, Encoders.bean(Buys.class));
	
	}
	
	
	
	
	public abstract Dataset<pojo.Buys> getBuysList(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.CustomerAttribute> client_condition);
	
	public Dataset<pojo.Buys> getBuysListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		return getBuysList(order_condition, null);
	}
	
	public pojo.Buys getBuysByOrder(pojo.Order order) {
		conditions.Condition<conditions.OrderAttribute> cond = null;
		cond = conditions.Condition.simple(conditions.OrderAttribute.id, conditions.Operator.EQUALS, order.getId());
		Dataset<pojo.Buys> res = getBuysListByOrderCondition(cond);
		List<pojo.Buys> list = res.collectAsList();
		if(list.size() > 0)
			return list.get(0);
		else
			return null;
	}
	public Dataset<pojo.Buys> getBuysListByClientCondition(
		conditions.Condition<conditions.CustomerAttribute> client_condition
	){
		return getBuysList(null, client_condition);
	}
	
	public Dataset<pojo.Buys> getBuysListByClient(pojo.Customer client) {
		conditions.Condition<conditions.CustomerAttribute> cond = null;
		cond = conditions.Condition.simple(conditions.CustomerAttribute.id, conditions.Operator.EQUALS, client.getId());
		Dataset<pojo.Buys> res = getBuysListByClientCondition(cond);
	return res;
	}
	
	
	
	public abstract void deleteBuysList(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.CustomerAttribute> client_condition);
	
	public void deleteBuysListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteBuysList(order_condition, null);
	}
	
	public void deleteBuysByOrder(pojo.Order order) {
		// TODO using id for selecting
		return;
	}
	public void deleteBuysListByClientCondition(
		conditions.Condition<conditions.CustomerAttribute> client_condition
	){
		deleteBuysList(null, client_condition);
	}
	
	public void deleteBuysListByClient(pojo.Customer client) {
		// TODO using id for selecting
		return;
	}
		
}
