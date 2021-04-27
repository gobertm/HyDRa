package conditions;

import pojo.IPojo;

public class OrCondition<E> extends Condition<E> {

	private Condition<E> leftCondition;
	private Condition<E> rightCondition;

	public OrCondition(Condition<E> leftCondition, Condition<E> rightCondition) {
		setLeftCondition(leftCondition);
		setRightCondition(rightCondition);
	}

	public Condition<E> getLeftCondition() {
		return this.leftCondition;
	}

	public void setLeftCondition(Condition<E> leftCondition) {
		this.leftCondition = leftCondition;
	}

	public Condition<E> getRightCondition() {
		return this.rightCondition;
	}

	public void setRightCondition(Condition<E> rightCondition) {
		this.rightCondition = rightCondition;
	}

	@Override
	public Class<E> eval() throws Exception {
		Class<E> cl1 = leftCondition.eval();
		Class<E> cl2 = rightCondition.eval();

		if(cl1 != cl2)
			throw new Exception("This condition is defined on more than one POJO class: " + cl1 + " and " + cl2);
		return cl1;
	}

	@Override
	public boolean evaluate(IPojo o) {
		if(getLeftCondition() == null && getRightCondition() == null)
			return true;
		boolean res = false;
		if(getLeftCondition() != null)
			res = getLeftCondition().evaluate(o);
		if(getRightCondition() != null)
			res = res || getRightCondition().evaluate(o);
		return res;
	}
}
