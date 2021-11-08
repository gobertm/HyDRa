package conditions;

import pojo.IPojo;
import java.io.Serializable;
import org.apache.commons.lang3.ArrayUtils;

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

	public static Condition createOrCondition(Attributes attribute, Operator operator, Object[] values) {
		if (values.length==1) {
			return Condition.simple(attribute, operator, values[0]);
		} else {
			return OrCondition.or(Condition.simple(attribute, operator, values[0]), createOrCondition(attribute, operator, ArrayUtils.remove(values,0)));
		}
	}
}
