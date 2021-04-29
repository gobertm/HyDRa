package dao.services;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import conditions.Condition;
import pojo.From;
import tdo.StoreTDO;
import tdo.FromTDO;
import pojo.Store;
import conditions.StoreAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;
import tdo.OrderTDO;
import tdo.FromTDO;
import pojo.Order;
import conditions.OrderAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;


public abstract class FromService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FromService.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	
	
	// Left side 'storeId' of reference [buys_in ]
	public abstract Dataset<OrderTDO> getOrderTDOListOrderInBuys_inInClientCollectionFromMongoSchema(Condition<OrderAttribute> condition, MutableBoolean refilterFlag);
	
	// Right side 'ID' of reference [buys_in ]
	public abstract Dataset<StoreTDO> getStoreTDOListStoreInBuys_inInClientCollectionFromMongoSchema(Condition<StoreAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	
	
	public abstract java.util.List<pojo.From> getFromList(
		conditions.Condition<conditions.StoreAttribute> store_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition);
	
	public java.util.List<pojo.From> getFromListByStoreCondition(
		conditions.Condition<conditions.StoreAttribute> store_condition
	){
		return getFromList(store_condition, null);
	}
	
	public java.util.List<pojo.From> getFromListByStore(pojo.Store store) {
		// TODO using id for selecting
		return null;
	}
	public java.util.List<pojo.From> getFromListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		return getFromList(null, order_condition);
	}
	
	public pojo.From getFromByOrder(pojo.Order order) {
		// TODO using id for selecting
		return null;
	}
	
	public abstract void insertFromAndLinkedItems(pojo.From from);
	
	
	
	public abstract void deleteFromList(
		conditions.Condition<conditions.StoreAttribute> store_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition);
	
	public void deleteFromListByStoreCondition(
		conditions.Condition<conditions.StoreAttribute> store_condition
	){
		deleteFromList(store_condition, null);
	}
	
	public void deleteFromListByStore(pojo.Store store) {
		// TODO using id for selecting
		return;
	}
	public void deleteFromListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deleteFromList(null, order_condition);
	}
	
	public void deleteFromByOrder(pojo.Order order) {
		// TODO using id for selecting
		return;
	}
		
}
