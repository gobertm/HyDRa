package dao.services;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import conditions.Condition;
import pojo.Places;
import tdo.CustomerTDO;
import tdo.PlacesTDO;
import pojo.Customer;
import pojo.Places;
import conditions.CustomerAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;
import tdo.OrderTDO;
import tdo.PlacesTDO;
import pojo.Order;
import pojo.Places;
import conditions.OrderAttribute;
import org.apache.commons.lang.mutable.MutableBoolean;


public abstract class PlacesService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PlacesService.class);
	
	
	
	/* Retrieve the Technical Data Object (TDO) for a Role in a mapped reference declared in a specific Abstract Physical Structure. 
		The entity mapped on the right hand side of the reference may be stored in another physical structure than where the ref is declared. 
		Leading to apparent inconsistency in the method name. But it is actually the physical structure of the ref and not the EntityDTO.*/
	
	// method accessing the embedded object orders mapped to role buyer
	public abstract Dataset<pojo.Places> getPlacesListInmongoSchemaClientCollectionorders(Condition<CustomerAttribute> buyer_condition, Condition<OrderAttribute> order_condition, MutableBoolean buyer_refilter, MutableBoolean order_refilter);
	
	
	
	
	public abstract java.util.List<pojo.Places> getPlacesList(
		conditions.Condition<conditions.CustomerAttribute> buyer_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition);
	
	public java.util.List<pojo.Places> getPlacesListByBuyerCondition(
		conditions.Condition<conditions.CustomerAttribute> buyer_condition
	){
		return getPlacesList(buyer_condition, null);
	}
	
	public java.util.List<pojo.Places> getPlacesListByBuyer(pojo.Customer buyer) {
		// TODO using id for selecting
		return null;
	}
	public java.util.List<pojo.Places> getPlacesListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		return getPlacesList(null, order_condition);
	}
	
	public pojo.Places getPlacesByOrder(pojo.Order order) {
		// TODO using id for selecting
		return null;
	}
	
	public abstract void insertPlaces(Places places);
	
	public abstract void deletePlacesList(
		conditions.Condition<conditions.CustomerAttribute> buyer_condition,
		conditions.Condition<conditions.OrderAttribute> order_condition);
	
	public void deletePlacesListByBuyerCondition(
		conditions.Condition<conditions.CustomerAttribute> buyer_condition
	){
		deletePlacesList(buyer_condition, null);
	}
	
	public void deletePlacesListByBuyer(pojo.Customer buyer) {
		// TODO using id for selecting
		return;
	}
	public void deletePlacesListByOrderCondition(
		conditions.Condition<conditions.OrderAttribute> order_condition
	){
		deletePlacesList(null, order_condition);
	}
	
	public void deletePlacesByOrder(pojo.Order order) {
		// TODO using id for selecting
		return;
	}
		
}
