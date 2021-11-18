package dao.services;

import util.Dataset;
import util.Row;
import util.WrappedArray;
import conditions.Condition;
import pojo.Composed_of;
import java.time.LocalDate;
import tdo.OrderTDO;
import tdo.Composed_ofTDO;
import pojo.Order;
import pojo.Composed_of;
import conditions.OrderAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.MapFunction;
import util.Util;
import tdo.ProductTDO;
import tdo.Composed_ofTDO;
import pojo.Product;
import pojo.Composed_of;
import conditions.ProductAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.MapFunction;
import util.Util;


public abstract class Composed_ofService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Composed_ofService.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	//join structure
	// Left side 'orderid' of reference [orderRef ]
	public abstract Dataset<OrderTDO> getOrderTDOListOrderPInOrderRefInOrderTableFromRelSchemaB(Condition<OrderAttribute> condition, MutableBoolean refilterFlag);
	// return join values orderRef productRef
	public abstract Dataset<Composed_ofTDO> getComposed_ofTDOListInComposed_of_OrderRef_ProductRefInDetailOrderColFromMongoSchemaB();
	
	
	
	
	
	//join structure
	// Left side 'productid' of reference [productRef ]
	public abstract Dataset<ProductTDO> getProductTDOListOrderedProductsInProductRefInProductsFromKvSchemaB(Condition<ProductAttribute> condition, MutableBoolean refilterFlag);
	// return join values productRef orderRef
	public abstract Dataset<Composed_ofTDO> getComposed_ofTDOListInComposed_of_ProductRef_OrderRefInDetailOrderColFromMongoSchemaB();
	
	
	
	
	
	
	public static Dataset<Composed_of> fullLeftOuterJoinBetweenComposed_ofAndOrderP(Dataset<Composed_of> d1, Dataset<Order> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("id", "A_id")
			.withColumnRenamed("orderdate", "A_orderdate")
			.withColumnRenamed("totalprice", "A_totalprice")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("orderP.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map(new MapFunction<Row, Composed_of>() {
			public Composed_of call(Row r) {
				Composed_of res = new Composed_of();
	
				Order orderP = new Order();
				Object o = r.getAs("orderP");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						orderP.setId(Util.getStringValue(r2.getAs("id")));
						orderP.setOrderdate(Util.getLocalDateValue(r2.getAs("orderdate")));
						orderP.setTotalprice(Util.getDoubleValue(r2.getAs("totalprice")));
					} 
					if(o instanceof Order) {
						orderP = (Order) o;
					}
				}
	
				res.setOrderP(orderP);
	
				String id = Util.getStringValue(r.getAs("A_id"));
				if (orderP.getId() != null && id != null && !orderP.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.id': " + orderP.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.id': " + orderP.getId() + " and " + id + "." );
				}
				if(id != null)
					orderP.setId(id);
				LocalDate orderdate = Util.getLocalDateValue(r.getAs("A_orderdate"));
				if (orderP.getOrderdate() != null && orderdate != null && !orderP.getOrderdate().equals(orderdate)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.orderdate': " + orderP.getOrderdate() + " and " + orderdate + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.orderdate': " + orderP.getOrderdate() + " and " + orderdate + "." );
				}
				if(orderdate != null)
					orderP.setOrderdate(orderdate);
				Double totalprice = Util.getDoubleValue(r.getAs("A_totalprice"));
				if (orderP.getTotalprice() != null && totalprice != null && !orderP.getTotalprice().equals(totalprice)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.totalprice': " + orderP.getTotalprice() + " and " + totalprice + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.totalprice': " + orderP.getTotalprice() + " and " + totalprice + "." );
				}
				if(totalprice != null)
					orderP.setTotalprice(totalprice);
	
				o = r.getAs("orderedProducts");
				Product orderedProducts = new Product();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						orderedProducts.setId(Util.getStringValue(r2.getAs("id")));
						orderedProducts.setTitle(Util.getStringValue(r2.getAs("title")));
						orderedProducts.setPrice(Util.getDoubleValue(r2.getAs("price")));
						orderedProducts.setPhoto(Util.getStringValue(r2.getAs("photo")));
					} 
					if(o instanceof Product) {
						orderedProducts = (Product) o;
					}
				}
	
				res.setOrderedProducts(orderedProducts);
	
				return res;
			}
					
		}, Encoders.bean(Composed_of.class));
	
		
		
	}
	public static Dataset<Composed_of> fullLeftOuterJoinBetweenComposed_ofAndOrderedProducts(Dataset<Composed_of> d1, Dataset<Product> d2) {
		Dataset<Row> d2_ = d2
			.withColumnRenamed("id", "A_id")
			.withColumnRenamed("title", "A_title")
			.withColumnRenamed("price", "A_price")
			.withColumnRenamed("photo", "A_photo")
			.withColumnRenamed("logEvents", "A_logEvents");
		
		Column joinCond = null;
		joinCond = d1.col("orderedProducts.id").equalTo(d2_.col("A_id"));
	
		d2_ = d1.join(d2_, joinCond, "leftouter");
		return d2_.map(new MapFunction<Row, Composed_of>() {
			public Composed_of call(Row r) {
				Composed_of res = new Composed_of();
	
				Product orderedProducts = new Product();
				Object o = r.getAs("orderedProducts");
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						orderedProducts.setId(Util.getStringValue(r2.getAs("id")));
						orderedProducts.setTitle(Util.getStringValue(r2.getAs("title")));
						orderedProducts.setPrice(Util.getDoubleValue(r2.getAs("price")));
						orderedProducts.setPhoto(Util.getStringValue(r2.getAs("photo")));
					} 
					if(o instanceof Product) {
						orderedProducts = (Product) o;
					}
				}
	
				res.setOrderedProducts(orderedProducts);
	
				String id = Util.getStringValue(r.getAs("A_id"));
				if (orderedProducts.getId() != null && id != null && !orderedProducts.getId().equals(id)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.id': " + orderedProducts.getId() + " and " + id + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.id': " + orderedProducts.getId() + " and " + id + "." );
				}
				if(id != null)
					orderedProducts.setId(id);
				String title = Util.getStringValue(r.getAs("A_title"));
				if (orderedProducts.getTitle() != null && title != null && !orderedProducts.getTitle().equals(title)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.title': " + orderedProducts.getTitle() + " and " + title + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.title': " + orderedProducts.getTitle() + " and " + title + "." );
				}
				if(title != null)
					orderedProducts.setTitle(title);
				Double price = Util.getDoubleValue(r.getAs("A_price"));
				if (orderedProducts.getPrice() != null && price != null && !orderedProducts.getPrice().equals(price)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.price': " + orderedProducts.getPrice() + " and " + price + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.price': " + orderedProducts.getPrice() + " and " + price + "." );
				}
				if(price != null)
					orderedProducts.setPrice(price);
				String photo = Util.getStringValue(r.getAs("A_photo"));
				if (orderedProducts.getPhoto() != null && photo != null && !orderedProducts.getPhoto().equals(photo)) {
					res.addLogEvent("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.photo': " + orderedProducts.getPhoto() + " and " + photo + "." );
					logger.warn("Data consistency problem for [Composed_of - different values found for attribute 'Composed_of.photo': " + orderedProducts.getPhoto() + " and " + photo + "." );
				}
				if(photo != null)
					orderedProducts.setPhoto(photo);
	
				o = r.getAs("orderP");
				Order orderP = new Order();
				if(o != null) {
					if(o instanceof Row) {
						Row r2 = (Row) o;
						orderP.setId(Util.getStringValue(r2.getAs("id")));
						orderP.setOrderdate(Util.getLocalDateValue(r2.getAs("orderdate")));
						orderP.setTotalprice(Util.getDoubleValue(r2.getAs("totalprice")));
					} 
					if(o instanceof Order) {
						orderP = (Order) o;
					}
				}
	
				res.setOrderP(orderP);
	
				return res;
			}
					
		}, Encoders.bean(Composed_of.class));
	
		
		
	}
	
	public static Dataset<Composed_of> fullOuterJoinsComposed_of(List<Dataset<Composed_of>> datasetsPOJO) {
		return fullOuterJoinsComposed_of(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Composed_of> fullLeftOuterJoinsComposed_of(List<Dataset<Composed_of>> datasetsPOJO) {
		return fullOuterJoinsComposed_of(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Composed_of> fullOuterJoinsComposed_of(List<Dataset<Composed_of>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		List<String> idFields = new ArrayList<String>();
		idFields.add("orderP.id");
	
		idFields.add("orderedProducts.id");
		scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
		
		List<Dataset<Row>> rows = new ArrayList<Dataset<Row>>();
		for(int i = 0; i < datasetsPOJO.size(); i++) {
			Dataset<Composed_of> d = datasetsPOJO.get(i);
			rows.add(d
				.withColumn("orderP_id_" + i, d.col("orderP.id"))
				.withColumn("orderedProducts_id_" + i, d.col("orderedProducts.id"))
				.withColumnRenamed("orderP", "orderP_" + i)
				.withColumnRenamed("orderedProducts", "orderedProducts_" + i)
				.withColumnRenamed("logEvents", "logEvents_" + i));
		}
		
		Column joinCond;
		joinCond = rows.get(0).col("orderP_id_0").equalTo(rows.get(1).col("orderP_id_1"));
		joinCond = joinCond.and(rows.get(0).col("orderedProducts_id_0").equalTo(rows.get(1).col("orderedProducts_id_1")));
		
		Dataset<Row> res = rows.get(0).join(rows.get(1), joinCond, joinMode);
		for(int i = 2; i < rows.size(); i++) {
			joinCond = rows.get(i - 1).col("orderP_id_" + (i - 1)).equalTo(rows.get(i).col("orderP_id_" + i));
			joinCond = joinCond.and(rows.get(i - 1).col("orderedProducts_id_" + (i - 1)).equalTo(rows.get(i).col("orderedProducts_id_" + i)));
			res = res.join(rows.get(i), joinCond, joinMode);
		}
	
		return res.map(new MapFunction<Row, Composed_of>() {
			public Composed_of call(Row r) {
				Composed_of composed_of_res = new Composed_of();
	
					WrappedArray logEvents = r.getAs("logEvents_0");
					if(logEvents != null)
						for (int i = 0; i < logEvents.size(); i++){
							composed_of_res.addLogEvent((String) logEvents.apply(i));
						}
		
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						logEvents = r.getAs("logEvents_" + i);
						if(logEvents != null)
						for (int j = 0; j < logEvents.size(); j++){
							composed_of_res.addLogEvent((String) logEvents.apply(j));
						}
					}
	
					Order orderP = new Order();
					orderP.setId(Util.getStringValue(r.getAs("orderP_0.id")));
					orderP.setOrderdate(Util.getLocalDateValue(r.getAs("orderP_0.orderdate")));
					orderP.setTotalprice(Util.getDoubleValue(r.getAs("orderP_0.totalprice")));
	
					Product orderedProducts = new Product();
					orderedProducts.setId(Util.getStringValue(r.getAs("orderedProducts_0.id")));
					orderedProducts.setTitle(Util.getStringValue(r.getAs("orderedProducts_0.title")));
					orderedProducts.setPrice(Util.getDoubleValue(r.getAs("orderedProducts_0.price")));
					orderedProducts.setPhoto(Util.getStringValue(r.getAs("orderedProducts_0.photo")));
	
					composed_of_res.setOrderP(orderP);
					composed_of_res.setOrderedProducts(orderedProducts);
					return composed_of_res;
			}
		}
		, Encoders.bean(Composed_of.class));
	
	}
	
	
	
	
	public abstract Dataset<pojo.Composed_of> getComposed_ofList(
		conditions.Condition<conditions.OrderAttribute> orderP_condition,
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition);
	
	public Dataset<pojo.Composed_of> getComposed_ofListByOrderPCondition(
		conditions.Condition<conditions.OrderAttribute> orderP_condition
	){
		return getComposed_ofList(orderP_condition, null);
	}
	
	public Dataset<pojo.Composed_of> getComposed_ofListByOrderP(pojo.Order orderP) {
		conditions.Condition<conditions.OrderAttribute> cond = null;
		cond = conditions.Condition.simple(conditions.OrderAttribute.id, conditions.Operator.EQUALS, orderP.getId());
		Dataset<pojo.Composed_of> res = getComposed_ofListByOrderPCondition(cond);
	return res;
	}
	public Dataset<pojo.Composed_of> getComposed_ofListByOrderedProductsCondition(
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition
	){
		return getComposed_ofList(null, orderedProducts_condition);
	}
	
	public Dataset<pojo.Composed_of> getComposed_ofListByOrderedProducts(pojo.Product orderedProducts) {
		conditions.Condition<conditions.ProductAttribute> cond = null;
		cond = conditions.Condition.simple(conditions.ProductAttribute.id, conditions.Operator.EQUALS, orderedProducts.getId());
		Dataset<pojo.Composed_of> res = getComposed_ofListByOrderedProductsCondition(cond);
	return res;
	}
	
	public abstract void insertComposed_of(Composed_of composed_of);
	
	public 	abstract boolean insertComposed_ofInJoinStructDetailOrderColInMongoModelB(Composed_of composed_of);
	
	
	
	 public void insertComposed_of(Order orderP ,Product orderedProducts ){
		Composed_of composed_of = new Composed_of();
		composed_of.setOrderP(orderP);
		composed_of.setOrderedProducts(orderedProducts);
		insertComposed_of(composed_of);
	}
	
	 public void insertComposed_of(Product product, List<Order> orderPList){
		Composed_of composed_of = new Composed_of();
		composed_of.setOrderedProducts(product);
		for(Order orderP : orderPList){
			composed_of.setOrderP(orderP);
			insertComposed_of(composed_of);
		}
	}
	 public void insertComposed_of(Order order, List<Product> orderedProductsList){
		Composed_of composed_of = new Composed_of();
		composed_of.setOrderP(order);
		for(Product orderedProducts : orderedProductsList){
			composed_of.setOrderedProducts(orderedProducts);
			insertComposed_of(composed_of);
		}
	}
	
	
	public abstract void deleteComposed_ofList(
		conditions.Condition<conditions.OrderAttribute> orderP_condition,
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition);
	
	public void deleteComposed_ofListByOrderPCondition(
		conditions.Condition<conditions.OrderAttribute> orderP_condition
	){
		deleteComposed_ofList(orderP_condition, null);
	}
	
	public void deleteComposed_ofListByOrderP(pojo.Order orderP) {
		// TODO using id for selecting
		return;
	}
	public void deleteComposed_ofListByOrderedProductsCondition(
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition
	){
		deleteComposed_ofList(null, orderedProducts_condition);
	}
	
	public void deleteComposed_ofListByOrderedProducts(pojo.Product orderedProducts) {
		// TODO using id for selecting
		return;
	}
		
}
