package conditions;

import pojo.*;

public class SimpleCondition<E> extends Condition<E> {

	private E attribute;
	private Operator operator;
	private Object value;

	public SimpleCondition(E attribute, Operator operator, Object value) {
		setAttribute(attribute);
		setOperator(operator);
		setValue(value);
	}

	public E getAttribute() {
		return this.attribute;
	}

	public void setAttribute(E attribute) {
		this.attribute = attribute;
	}

	public Operator getOperator() {
		return this.operator;
	}

	public void setOperator(Operator operator) {
		this.operator = operator;
	}

	public Object getValue() {
		return this.value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	@Override
	public Class<E> eval() throws Exception {
		if(getOperator() == null)
			throw new Exception("You cannot specify a NULL operator in a simple condition");
		if(getValue() == null && operator != Operator.EQUALS && operator != Operator.NOT_EQUALS)
			throw new Exception("You cannot specify a NULL value with this operator");

		return (Class<E>) attribute.getClass();
	}

	@Override
	public boolean evaluate(IPojo obj) {
		if(obj instanceof Customer)
			return evaluateCustomer((Customer) obj);
		if(obj instanceof Product)
			return evaluateProduct((Product) obj);
		if(obj instanceof Order)
			return evaluateOrder((Order) obj);
		if(obj instanceof Feedback)
			return evaluateFeedback((Feedback) obj);
		return true;
	}


	private boolean evaluateCustomer(Customer obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		CustomerAttribute attr = (CustomerAttribute) this.attribute;
		Object objectValue = null;

		if(attr == CustomerAttribute.id)
			objectValue = obj.getId();
		if(attr == CustomerAttribute.firstname)
			objectValue = obj.getFirstname();
		if(attr == CustomerAttribute.lastname)
			objectValue = obj.getLastname();
		if(attr == CustomerAttribute.gender)
			objectValue = obj.getGender();
		if(attr == CustomerAttribute.birthday)
			objectValue = obj.getBirthday();
		if(attr == CustomerAttribute.creationDate)
			objectValue = obj.getCreationDate();
		if(attr == CustomerAttribute.locationip)
			objectValue = obj.getLocationip();
		if(attr == CustomerAttribute.browser)
			objectValue = obj.getBrowser();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateProduct(Product obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		ProductAttribute attr = (ProductAttribute) this.attribute;
		Object objectValue = null;

		if(attr == ProductAttribute.id)
			objectValue = obj.getId();
		if(attr == ProductAttribute.title)
			objectValue = obj.getTitle();
		if(attr == ProductAttribute.price)
			objectValue = obj.getPrice();
		if(attr == ProductAttribute.photo)
			objectValue = obj.getPhoto();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateOrder(Order obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		OrderAttribute attr = (OrderAttribute) this.attribute;
		Object objectValue = null;

		if(attr == OrderAttribute.id)
			objectValue = obj.getId();
		if(attr == OrderAttribute.orderdate)
			objectValue = obj.getOrderdate();
		if(attr == OrderAttribute.totalprice)
			objectValue = obj.getTotalprice();

		return operator.evaluate(objectValue, this.getValue());
	}
		private boolean evaluateFeedback(Feedback obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		FeedbackAttribute attr = (FeedbackAttribute) this.attribute;
		Object objectValue = null;

		if(attr == FeedbackAttribute.rate)
			objectValue = obj.getRate();
		if(attr == FeedbackAttribute.content)
			objectValue = obj.getContent();

		return operator.evaluate(objectValue, this.getValue());
	}

	
}
