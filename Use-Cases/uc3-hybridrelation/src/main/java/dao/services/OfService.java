package dao.services;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import conditions.Condition;
import pojo.Of;
import tdo.ProductTDO;
import tdo.OfTDO;
import pojo.Product;
import pojo.Of;
import conditions.ProductAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;
import tdo.OrderTDO;
import tdo.OfTDO;
import pojo.Order;
import pojo.Of;
import conditions.OrderAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;


public abstract class OfService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OfService.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	
	
	// Left side 'productId' of reference [buys ]
	public abstract Dataset<OrderTDO> getOrderTDOListOrderInBuysInClientCollectionFromMongoSchema(Condition<OrderAttribute> condition, MutableBoolean refilterFlag);
	
	// Right side 'ID' of reference [buys ]
	public abstract Dataset<ProductTDO> getProductTDOListBought_itemInBuysInClientCollectionFromMongoSchema(Condition<ProductAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	
	
	public abstract java.util.List<pojo.Of> getOfList(
		conditions.Condition<conditions.ProductAttribute> bought_item_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition);
	
	public java.util.List<pojo.Of> getOfListByBought_itemCondition(
		conditions.Condition<conditions.ProductAttribute> bought_item_condition
	){
		return getOfList(bought_item_condition, null);
	}
	
	public java.util.List<pojo.Of> getOfListByBought_item(pojo.Product bought_item) {
		// TODO using id for selecting
		return null;
	}
	public java.util.List<pojo.Of> getOfListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		return getOfList(null, order_condition);
	}
	
	public pojo.Of getOfByOrder(pojo.Order order) {
		// TODO using id for selecting
		return null;
	}
	
	public abstract void insertOf(Of of);
	
	public abstract void deleteOfList(
		conditions.Condition<conditions.ProductAttribute> bought_item_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition);
	
	public void deleteOfListByBought_itemCondition(
		conditions.Condition<conditions.ProductAttribute> bought_item_condition
	){
		deleteOfList(bought_item_condition, null);
	}
	
	public void deleteOfListByBought_item(pojo.Product bought_item) {
		// TODO using id for selecting
		return;
	}
	public void deleteOfListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteOfList(null, order_condition);
	}
	
	public void deleteOfByOrder(pojo.Order order) {
		// TODO using id for selecting
		return;
	}
		
}
