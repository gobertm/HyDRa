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
		if(obj instanceof Product)
			return evaluateProduct((Product) obj);
		return true;
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
		if(attr == ProductAttribute.Name)
			objectValue = obj.getName();
		if(attr == ProductAttribute.photo)
			objectValue = obj.getPhoto();
		if(attr == ProductAttribute.price)
			objectValue = obj.getPrice();
		if(attr == ProductAttribute.description)
			objectValue = obj.getDescription();
		if(attr == ProductAttribute.category)
			objectValue = obj.getCategory();

		return operator.evaluate(objectValue, this.getValue());
	}

	
}
