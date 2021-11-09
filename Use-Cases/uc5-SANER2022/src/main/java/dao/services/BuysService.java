package dao.services;

import util.Dataset;
import util.Row;
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
import tdo.CustomerTDO;
import tdo.BuysTDO;
import pojo.Customer;
import pojo.Buys;
import conditions.CustomerAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;
import java.util.List;


public abstract class BuysService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BuysService.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	
	
	// Left side 'PersonId' of reference [customer ]
	public abstract Dataset<OrderTDO> getOrderTDOListOrderInCustomerInOrdersColFromDocSchema(Condition<OrderAttribute> condition, MutableBoolean refilterFlag);
	
	// Right side 'id' of reference [customer ]
	public abstract Dataset<CustomerTDO> getCustomerTDOListClientInCustomerInOrdersColFromDocSchema(Condition<CustomerAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	
	
	public abstract java.util.List<pojo.Buys> getBuysList(
		conditions.Condition<conditions.OrderAttribute> order_condition,
		conditions.Condition<conditions.CustomerAttribute> client_condition);
	
	public java.util.List<pojo.Buys> getBuysListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		return getBuysList(order_condition, null);
	}
	
	public pojo.Buys getBuysByOrder(pojo.Order order) {
		// TODO using id for selecting
		return null;
	}
	public java.util.List<pojo.Buys> getBuysListByClientCondition(
		conditions.Condition<conditions.CustomerAttribute> client_condition
	){
		return getBuysList(null, client_condition);
	}
	
	public java.util.List<pojo.Buys> getBuysListByClient(pojo.Customer client) {
		// TODO using id for selecting
		return null;
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
