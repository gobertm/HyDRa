package util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.bson.Document;
import org.codehaus.plexus.util.StringUtils;

public class Row {
	private Map<String, Object> fieldValues = null;
	private Document doc = null;

	public Row(Document doc) {
		this.doc = doc;
	}

	public Row(Map<String, Object> fieldValues) {
		if (fieldValues == null)
			return;
		Map<String, Object> res = new HashMap<String, Object>();
		for (Entry<String, Object> entry : fieldValues.entrySet()) {
			Object value = entry.getValue();
			if (value instanceof List) {
				value = new WrappedArray((List) value);
			}

			res.put(entry.getKey(), value);

		}

		this.fieldValues = res;
	}

	public String toString() {
		if (doc != null)
			return doc.toString();
		if (fieldValues != null)
			return fieldValues.toString();

		return null;
	}

	public void replaceFieldName(String oldName, String newName) {
		if (fieldValues == null)
			return;

		Object value = fieldValues.get(oldName);
		fieldValues.remove(oldName);
		fieldValues.put(newName, value);
	}

	public <T> T getAs(String name) {
		Object res = null;
		if (doc != null) {
			Object o = getAsInDoc(name);
			if (o instanceof List)
				res = new WrappedArray((List) o);
			else
				if(o instanceof Document)
					res = new Row((Document) o);
				else
					res = o;
		}

		if (fieldValues != null) {
			String[] calls = name.split("\\.");
			res = fieldValues.get(calls[0]);
			// ex: movie.id => getMovie().getId()
			for (int i = 1; i < calls.length; i++) {
				try {
					res = callGetter(res, calls[i]);
				} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException
						| NoSuchMethodException | SecurityException e) {
					e.printStackTrace();
				}
			}
		}

		return (T) res;
	}

	public static Object callGetter(Object o, String getter) throws IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException, SecurityException {
		if (o == null)
			return null;

		return Row.getMethod("get" + StringUtils.capitalise(getter), o.getClass()).invoke(o);

	}

	private Object getAsInDoc(String name) {
		return doc.get(name);
	}

	public Schema schema() {
		if (doc != null)
			return new Schema(doc.keySet());
		if (fieldValues != null)
			return new Schema(fieldValues.keySet());

		return null;
	}

	public static class Schema {
		private String[] fieldNames;

		public Schema(Set<String> fieldNames) {
			this.fieldNames = new String[fieldNames.size()];
			int i = 0;
			for (String str : fieldNames) {
				this.fieldNames[i] = str;
				i++;
			}
		}

		public String[] fieldNames() {
			return fieldNames;
		}
	}

	public Map<String, Object> getFieldValues() {
		return fieldValues;
	}

	public static Method getMethod(String name, Class c, Class... params) {
		if (c == null)
			return null;
		Method res = null;
		try {
			res = c.getDeclaredMethod(name, params);
		} catch (NoSuchMethodException | SecurityException e) {
			if (c.getSuperclass() != null)
				res = getMethod(name, c.getSuperclass(), params);
			if (res == null)
				for (Class interfac : c.getInterfaces()) {
					res = getMethod(name, interfac, params);
					if (res != null)
						break;
				}
		}

		return res;

	}

}

