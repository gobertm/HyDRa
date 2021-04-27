package conditions;

import pojo.IPojo;
import java.io.Serializable;

public abstract class Condition<E> implements Serializable {

	public static <E> SimpleCondition<E> simple(E attr, Operator op, Object value) {
		return new SimpleCondition<E>(attr, op, value);
	}

	public static <E> AndCondition<E> and(Condition<E> left, Condition<E> right) {
		return new AndCondition<E>(left, right);
	}
	
	public static <E> OrCondition<E> or(Condition<E> left, Condition<E> right) {
		return new OrCondition<E>(left, right);
	}

	public abstract Class<E> eval() throws Exception;

	public abstract boolean evaluate(IPojo obj);
}
