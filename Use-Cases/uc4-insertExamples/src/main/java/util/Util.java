package util;

import conditions.*;
import exceptions.IncompatibleAttributesTypeException;
import java.util.HashSet;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Set;

public class Util {
	
	/**
	Les prochaines methodes transformXXXValue ont pour but de transformer 
	l'object de la condition dans un format String conforme � la techno cible
	ex: un boolean doit-il etre traduit en true ou 1?
	ex2: une date, comment la formatter? etc.
	Cela dependra du type de la valeur ainsi que du backend cible
	**/
	public static String transformSQLValue(Object o) {
		if(o == null)
			return null;

		//TODO comportement different en fct des types de donnees(ex: dates et leur format)
		return o.toString();
	}

	public static String transformBSONValue(Object o) {
			if(o == null)
				return null;
	
			//TODO comportement different en fct des types de donnees (ex: dates et leur format)
			return o.toString();
	}

	public static String escapeReservedCharSQL(String v) {
		if(v == null)
			return null;
		//TODO other reserved char in regex sql
		return v.replaceAll("_", "\\\\\\\\_").replaceAll("%", "\\\\\\\\%");
	}

	public static String escapeReservedRegexMongo(String v) {
		if(v == null)
			return null;
		//TODO https://perldoc.perl.org/perlre
		return v.replaceAll("\\*", "\\\\\\\\*").replaceAll("\\^", "\\\\\\\\^").replaceAll("\\$", "\\\\\\\\\\$");
	}

	public static String escapeQuote(String s) {
		if(s == null)
			return null;
		return s.replaceAll("'", "\\\\\\\\'");
	}

	
	public static String getDelimitedMongoValue(Class cl, String value) {
		if(value == null)
			return null;

		if(cl == String.class)
			return "'" + value.replaceAll("'","\\\\\\'") + "'";

		//TODO handle the other special data types, e.g., Byte, Date, ...
		return value;
		
	}

	public static Double getDoubleValue(Object o) {
		if(o==null)
			return null;
		if(o instanceof BigDecimal)
			return ((BigDecimal) o).doubleValue();
		if(o instanceof Float)
			return ((Float) o).doubleValue();
		if(o instanceof Short)
			return ((Short) o).doubleValue();
		if(o instanceof Double)
			return ((Double) o).doubleValue();
		if(o instanceof Integer)
			return ((Integer) o).doubleValue();
		if (o instanceof Long)
			return ((Long) o).doubleValue();
		if (o instanceof String)
			return Double.parseDouble((String) o);
		throw new IncompatibleAttributesTypeException("Conceptual attribute type in model file incompatible with found datatype in database");
	}

	public static Integer getIntegerValue(Object o) {
		if(o==null)
			return null;
		if(o instanceof BigDecimal)
			return ((BigDecimal) o).intValue();
		if(o instanceof Float)
			return ((Float) o).intValue();
		if(o instanceof Double)
			return ((Double) o).intValue();
		if(o instanceof Short)
			return ((Short) o).intValue();
		if(o instanceof Integer)
			return ((Integer) o).intValue();
		if (o instanceof Long)
			return ((Long) o).intValue();
		if(o instanceof String)
			return Integer.parseInt((String) o);
		throw new IncompatibleAttributesTypeException("Conceptual attribute type in model file incompatible with found datatype in database");
	}

	public static Long getLongValue(Object o) {
		if(o==null)
			return null;
		if(o instanceof BigDecimal)
			return ((BigDecimal) o).longValue();
		if(o instanceof Float)
			return ((Float) o).longValue();
		if(o instanceof Short)
			return ((Short) o).longValue();
		if(o instanceof Double)
			return ((Double) o).longValue();
		if(o instanceof Integer)
			return ((Integer) o).longValue();
		if (o instanceof Long)
			return ((Long) o).longValue();
		if(o instanceof String)
			return Long.parseLong((String)o);
		throw new IncompatibleAttributesTypeException("Conceptual attribute type in model file incompatible with found datatype in database");
	}

	public static String getStringValue(Object o) {
		if(o==null)
			return null;
		if(o instanceof BigDecimal)
			return ((BigDecimal) o).toString();
		if(o instanceof Float)
			return ((Float) o).toString();
		if(o instanceof Double)
			return ((Double) o).toString();
		if(o instanceof Short)
			return ((Short) o).toString();
		if(o instanceof Integer)
			return ((Integer) o).toString();
		if (o instanceof Long)
			return ((Long) o).toString();
		if(o instanceof String)
			return (String) o ;
		throw new IncompatibleAttributesTypeException("Conceptual attribute type in model file incompatible with found datatype in database");
	}

	public static Boolean getBooleanValue(Object o) {
		if(o==null)
			return null;
		if(o instanceof Boolean)
			return (Boolean) o;
		throw new IncompatibleAttributesTypeException("Conceptual attribute type in model file incompatible with found datatype in database");
	}

	public static Byte getByteValue(Object o) {
		if(o==null)
			return null;
		// TODO To improve
		return (Byte) o;
	}

	public static LocalDate getDateValue(Object o){
		if(o==null)
			return null;
		if(o instanceof java.sql.Date)
			return ((java.sql.Date) o).toLocalDate();
		if(o instanceof LocalDate)
			return (LocalDate) o;
		if(o instanceof Timestamp)
			return ((Timestamp) o).toLocalDateTime().toLocalDate();
		throw new IncompatibleAttributesTypeException("Conceptual attribute type in model file incompatible with found datatype in database");
	}

	public static <E> Object getValueOfAttributeInEqualCondition(Condition<E> condition, E attribute) {
		Object oleft, oright;
		if (condition instanceof SimpleCondition) {
			SimpleCondition cond = ((SimpleCondition<E>) condition);
			if (cond.getOperator() == Operator.EQUALS) {
				if(cond.getAttribute()==attribute)
					return cond.getValue();
			}
		}
		if(condition instanceof AndCondition){
			AndCondition cond = (AndCondition) condition;
			oleft= getValueOfAttributeInEqualCondition(cond.getLeftCondition(), attribute);
			oright= getValueOfAttributeInEqualCondition(cond.getRightCondition(), attribute);
			if(oleft!=null)
				return oleft;
			if(oright!=null)
				return oright;
		}
		if(condition instanceof OrCondition){
			OrCondition cond = (OrCondition) condition;
			oleft= getValueOfAttributeInEqualCondition(cond.getLeftCondition(), attribute);
			oright= getValueOfAttributeInEqualCondition(cond.getRightCondition(), attribute);
			if(oleft!=null)
				return oleft;
			if(oright!=null)
				return oright;
		}
		return null;
	}

	public static boolean containsOrCondition(Condition condition) {
		boolean left=false, right=false;
		AndCondition andCondition;
		if(condition instanceof OrCondition)
			return true;
		if (condition instanceof AndCondition) {
			andCondition = (AndCondition) condition;
			left = containsOrCondition(andCondition.getLeftCondition());
			right = containsOrCondition(andCondition.getRightCondition());
			return left || right;
		}
		return false;
	}

	public static <E> Set<E> getConditionAttributes(Condition<E> condition) {
		Set<E> attributes = new HashSet<>();
		if (condition instanceof SimpleCondition) {
			SimpleCondition simpleCondition = (SimpleCondition) condition;
			attributes.add((E) simpleCondition.getAttribute());
		}
		if (condition instanceof OrCondition) {
			OrCondition orCondition = (OrCondition) condition;
			attributes.addAll(getConditionAttributes(orCondition.getLeftCondition()));
			attributes.addAll(getConditionAttributes(orCondition.getRightCondition()));
		}
		if (condition instanceof AndCondition) {
			AndCondition andCondition = (AndCondition) condition;
			attributes.addAll(getConditionAttributes(andCondition.getLeftCondition()));
			attributes.addAll(getConditionAttributes(andCondition.getRightCondition()));
		}
		return attributes;
	}

}
