package util;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoder;
import org.bson.Document;
import org.codehaus.plexus.util.StringUtils;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import pojo.IPojo;
import scala.collection.Seq;

public class Dataset<E> extends ArrayList<E> {
	private String alias = null;

	public void show() {
		for (int i = 0; i < 10 && i < size(); i++)
			System.out.println(get(i));
	}

	public List<E> collectAsList() {
		return this;
	}

	public int count() {
		return size();
	}

	public Dataset<E> as(String alias) {
		this.alias = alias;
		return this;
	}

	public E first() {
		return get(0);
	}

	public Dataset<E> dropDuplicates() {
		return dropDuplicates(null);
	}

	public Dataset<E> dropDuplicates(String[] fieldNames) {
		if (fieldNames == null || fieldNames.length == 0 || this.size() == 0)
			return this;

		Object first = this.get(0);
		Class cl = first.getClass();
		Method[] methods = new Method[fieldNames.length];
		for (int i = 0; i < fieldNames.length; i++) {
			Method m = null;
			if (first instanceof IPojo)
				m = Row.getMethod("get" + StringUtils.capitalise(fieldNames[i]), cl);
			if (first instanceof Row)
				m = Row.getMethod("getAs", cl, String.class);
			methods[i] = m;
		}

		Map<String, E> map = new HashMap<String, E>();
		Dataset<E> res = new Dataset<E>();
		for (E e : this) {
			String key = "";
			for (int i = 0; i < methods.length; i++) {
				try {
					Method m = methods[i];
					String fieldName = fieldNames[i];
					Object o = null;
					if (first instanceof IPojo)
						o = m.invoke(e);
					if (first instanceof Row)
						o = m.invoke(e, fieldName);
					if (o != null)
						key += o.toString();
					key += ";";
				} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e1) {
					e1.printStackTrace();
				}
			}

			if (map.put(key, e) == null)
				res.add(e);
		}

		return res;
	}

	public Dataset<E> filter(FilterFunction<E> fct) {
		for (int i = 0; i < this.size(); i++) {
			E e = this.get(i);
			try {
				if (!fct.call(e)) {
					this.remove(i);
					i--;
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}

		return this;

	}

	public <E2> Dataset<E2> flatMap(FlatMapFunction<E, E2> fct, Encoder encoder) {
		Dataset<E2> res = new Dataset<E2>();
		for (E e : this)
			try {
				Iterator<E2> it = fct.call(e);
				while (it.hasNext())
					res.add(it.next());
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		return res;

	}

	public <E2> Dataset<E2> map(MapFunction<E, E2> fct, Encoder encoder) {
		Dataset<E2> res = new Dataset<E2>();
		for (E e : this)
			try {
				E2 e2 = fct.call(e);
				res.add(e2);
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		return res;
	}

	public Column col(String colName) {
		return new Column(colName);
	}

	public Dataset<E> union(Dataset<E> d) {
		if(d == null)
			return this;

		Dataset<E> res = new Dataset<E>();
		res.addAll(this);
		res.addAll(d);

		return res;
	}

	public <E2> Dataset<Row> join(Dataset<E2> d) {
		return join(d, null);
	}

	public <E2> Dataset<Row> join(Dataset<E2> d, Seq<String> seq, String joinMode) {
		Column column = null;
		if (seq != null && seq.size() > 0) {
			column = this.col(seq.apply(0)).equalTo(d.col(seq.apply(0)));
			for (int i = 1; i < seq.size(); i++) {
				column = column.and(this.col(seq.apply(i)).equalTo(d.col(seq.apply(i))));
			}
		}

		return join(d, column, joinMode);
	}

	public <E2> Dataset<Row> join(Dataset<E2> d, Column joinCondition) {
		return join(d, joinCondition, "inner");
	}

	private <E2> Dataset<Row> fullOuterJoin(Dataset<E2> d, Column joinCondition) {
		Dataset<Row> res = new Dataset<Row>();
		for (E e : this) {
			Row r = null;
			for (E2 e2 : d) {
				try {
					if (evaluate(e, e2, joinCondition)) {
						r = merge(e, this.alias, e2, d.alias);
					}
				} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException
						| NoSuchMethodException | SecurityException | IntrospectionException e1) {
					e1.printStackTrace();
				}

			}

			if (r == null) {
				try {
					r = merge(e, this.alias, null, null);
				} catch (IntrospectionException e1) {
					e1.printStackTrace();
				}
			}

			res.add(r);
		}

		for (E2 e2 : d) {
			Row r = null;
			boolean found = false;
			for (E e : this) {
				try {
					if (evaluate(e, e2, joinCondition)) {
						found = true;
						break;
					}
				} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException
						| NoSuchMethodException | SecurityException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

			}

			if (!found) {
				try {
					r = merge(e2, d.alias, null, null);
					res.add(r);
				} catch (IntrospectionException e1) {
					e1.printStackTrace();
				}
			}

		}

		return res;
	}

	private <E2> Dataset<Row> leftOuterJoin(Dataset<E2> d, Column joinCondition) {
		Dataset<Row> res = new Dataset<Row>();
		for (E e : this) {
			Row r = null;
			for (E2 e2 : d) {
				try {
					if (evaluate(e, e2, joinCondition)) {
						r = merge(e, this.alias, e2, d.alias);
					}
				} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException
						| NoSuchMethodException | SecurityException | IntrospectionException e1) {
					e1.printStackTrace();
				}

			}

			if (r == null) {
				try {
					r = merge(e, this.alias, null, null);
				} catch (IntrospectionException e1) {
					e1.printStackTrace();
				}
			}

			res.add(r);
		}
		return res;
	}

	private <E2> Dataset<Row> innerJoin(Dataset<E2> d, Column joinCondition) {
		Dataset<Row> res = new Dataset<Row>();
		for (E e : this) {
			for (E2 e2 : d) {
				try {
					if (evaluate(e, e2, joinCondition)) {
						Row r = merge(e, this.alias, e2, d.alias);
						res.add(r);
					}
				} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException
						| NoSuchMethodException | SecurityException | IntrospectionException e1) {
					e1.printStackTrace();
				}

			}
		}
		return res;
	}

	public <E2> Dataset<Row> join(Dataset<E2> d, Column joinCondition, String joinMode) {
		if (joinMode == null)
			joinMode = "inner";

		switch (joinMode) {
		case "fullouter":
			return fullOuterJoin(d, joinCondition);
		case "leftouter":
			return leftOuterJoin(d, joinCondition);
		case "inner":
			return innerJoin(d, joinCondition);
		default:
			return null;
		}

	}

	private static <E1, E2> Row merge(E1 e1, String alias1, E2 e2, String alias2) throws IntrospectionException {
		Map<String, Object> map = getFieldValueCouple(e1);
		if (alias1 != null) {
			Row r1 = new Row(map);
			map = new HashMap<String, Object>();
			map.put(alias1, r1);
		}

		Map<String, Object> map2 = getFieldValueCouple(e2);
		if (alias2 != null) {
			Row r2 = new Row(map2);
			map2 = new HashMap<String, Object>();
			map2.put(alias2, r2);
		}

		map.putAll(map2);
		return new Row(map);
	}

	private static <E1> Map<String, Object> getFieldValueCouple(E1 e) {
		if (e == null)
			return new HashMap<String, Object>();

		if (e instanceof Row)
			return ((Row) e).getFieldValues();

		Map<String, Object> res = new HashMap<String, Object>();
		try {
			for (PropertyDescriptor propertyDescriptor : Introspector.getBeanInfo(e.getClass())
					.getPropertyDescriptors()) {
				Method m = propertyDescriptor.getReadMethod();
				if (m.getName().equals("getClass"))
					continue;
				String fieldName = m.getName().startsWith("get") ? StringUtils.uncapitalise(m.getName().substring(3))
						: (m.getName().startsWith("is") ? StringUtils.uncapitalise(m.getName().substring(2)) : null);
				Object value = m.invoke(e);
				res.put(fieldName, value);
			}
		} catch (IntrospectionException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e1) {
			e1.printStackTrace();
		}
		return res;
	}

	private static <E1, E2> boolean evaluate(E1 e1, E2 e2, Column joinCondition) throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		if (joinCondition == null)
			return e1.equals(e2);

		String cond = joinCondition.toString();
		cond = cond.replaceAll("\\(", "").replaceAll("\\)", "");
		String[] conditions = cond.split(" AND ");
		for (String condition : conditions) {
			if (condition.startsWith("array_contains")) {
				String regex = "array_contains(.*), (.*)";
				Pattern p = Pattern.compile(regex);
				java.util.regex.Matcher m = p.matcher(condition);
				if (m.find()) {
					String col1 = m.group(1);
					String col2 = m.group(2);

					Object o1, o2 = null;
					if (e1 instanceof Row)
						o1 = ((Row) e1).getAs(col1);
					else {
						o1 = Row.getMethod("get" + StringUtils.capitalise(col1), e1.getClass()).invoke(e1);

					}

					if (e2 instanceof Row) {
						o2 = ((Row) e2).getAs(col2);
					} else {
						String[] calls = col2.split("\\.");
						o2 = Row.getMethod("get" + StringUtils.capitalise(calls[0]), e2.getClass()).invoke(e2);
						for (int i = 1; i < calls.length; i++)
							o2 = Row.callGetter(o2, calls[i]);

					}

					if (o1 == null || o2 == null)
						return false;

					if (!(o1 instanceof List) && !(o1 instanceof WrappedArray))
						return false;

					if (o1 instanceof List && !((List) o1).contains(o2))
						return false;
					
					if (o1 instanceof WrappedArray && !((WrappedArray) o1).list().contains(o2))
						return false;

				} else
					System.err.println("Contains condition unknown");
			} else if (condition.contains("=")) {
				// X = Y
				String[] columns = condition.split(" = ");
				String col1 = columns[0];
				String col2 = columns[1];
				Object o1, o2 = null;
				if (e1 instanceof Row)
					o1 = ((Row) e1).getAs(col1);
				else {
					o1 = Row.getMethod("get" + StringUtils.capitalise(col1), e1.getClass()).invoke(e1);

				}

				if (e2 instanceof Row)
					o2 = ((Row) e2).getAs(col2);
				else {
					String[] calls = col2.split("\\.");
					o2 = Row.getMethod("get" + StringUtils.capitalise(calls[0]), e2.getClass()).invoke(e2);
					for (int i = 1; i < calls.length; i++)
						o2 = Row.callGetter(o2, calls[i]);

				}

				if (o1 == null || o2 == null)
					return false;
				if (!o1.equals(o2))
					return false;
			} else
				System.err.println("Unknown condition: " + condition);
		}

		return true;

	}

	public Dataset<Row> select(String... cols) {
		Map<String, String[]> calls = new HashMap<String, String[]>();
		for (String col : cols) {
			calls.put(col, col.split("\\."));
		}

		Dataset<Row> res = new Dataset<Row>();
		for (E e : this) {
			Map<String, Object> rowMap = new HashMap<String, Object>();
			for (String col : cols) {
				Object o = e;
				boolean wildCard = false;
				String[] getters = calls.get(col);

				int i = 0;
				for (String getter : getters) {
					if (i == 0 && alias != null && alias.equals(getter)) // d.as("A").select("A.*")
						continue;
					if (getter.equals("*")) {
						rowMap.putAll(getFieldValueCouple(o));
						wildCard = true;
					} else {
						if (o != null && o instanceof Row)
							o = ((Row) o).getAs(getter);
						else {
							try {
								o = Row.callGetter(o, getter);
							} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException
									| NoSuchMethodException | SecurityException e1) {
								e1.printStackTrace();
							}
						}

					}

					i++;
				}

				if (!wildCard) {
					String field = getters[getters.length - 1];
					rowMap.put(field, o);
				}

			}

			res.add(new Row(rowMap));
		}

		return res;
	}

	public Dataset<Row> withColumnRenamed(String oldName, String newName) {
		Dataset<Row> res = new Dataset<Row>();

		for (E e : this) {
			Row r;
			if (e instanceof Row) {
				r = (Row) e;
			} else {
				r = new Row(getFieldValueCouple(e));
			}

			r.replaceFieldName(oldName, newName);

			res.add(r);
		}

		return res;
	}

	public <E1> Dataset<E1> as(Encoder<E1> encoder) {
		Dataset<E1> res = new Dataset<E1>();
		for (E e : this) {
			try {
				E1 e1 = (E1) encoder.clsTag().runtimeClass().getConstructor().newInstance();
				Map<String, Object> fieldValues = getFieldValueCouple(e);
				for (Entry<String, Object> entry : fieldValues.entrySet()) {
					String field = entry.getKey();
					Object value = entry.getValue();

					if (value != null) {
						Method m = getSetter(field, e1, value.getClass());
						if (m != null) {
							m.invoke(e1, value);
						}
					}
				}

				res.add(e1);

			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchMethodException | SecurityException e1) {
				e1.printStackTrace();
			}
		}
		return res;
	}

	private static Method getSetter(String fieldName, Object o, Class c) {
		return Row.getMethod("set" + StringUtils.capitalise(fieldName), o.getClass(), c);

	}

}

