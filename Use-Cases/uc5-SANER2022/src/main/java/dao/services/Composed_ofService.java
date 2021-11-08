package dao.services;

import util.Dataset;
import util.Row;
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
import tdo.ProductTDO;
import tdo.Composed_ofTDO;
import pojo.Product;
import pojo.Composed_of;
import conditions.ProductAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;
import java.util.List;


public abstract class Composed_ofService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Composed_ofService.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	
	// method accessing the embedded object Orderline mapped to role orderP
	public abstract Dataset<pojo.Composed_of> getComposed_ofListIndocSchemaordersColOrderline(Condition<OrderAttribute> orderP_condition, Condition<ProductAttribute> orderedProducts_condition, MutableBoolean orderP_refilter, MutableBoolean orderedProducts_refilter);
	
	
	
	
	public abstract java.util.List<pojo.Composed_of> getComposed_ofList(
		conditions.Condition<conditions.OrderAttribute> orderP_condition,
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition);
	
	public java.util.List<pojo.Composed_of> getComposed_ofListByOrderPCondition(
		conditions.Condition<conditions.OrderAttribute> orderP_condition
	){
		return getComposed_ofList(orderP_condition, null);
	}
	
	public java.util.List<pojo.Composed_of> getComposed_ofListByOrderP(pojo.Order orderP) {
		// TODO using id for selecting
		return null;
	}
	public java.util.List<pojo.Composed_of> getComposed_ofListByOrderedProductsCondition(
		conditions.Condition<conditions.ProductAttribute> orderedProducts_condition
	){
		return getComposed_ofList(null, orderedProducts_condition);
	}
	
	public java.util.List<pojo.Composed_of> getComposed_ofListByOrderedProducts(pojo.Product orderedProducts) {
		// TODO using id for selecting
		return null;
	}
	
	public abstract void insertComposed_of(Composed_of composed_of);
	
	
	public 	abstract boolean insertComposed_ofInEmbeddedStructOrdersColInMongobench(Composed_of composed_of);
	
	
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
