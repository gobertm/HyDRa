package conditions;

public class SetClause<E> {

	private java.util.Map<E, Object> sets = new java.util.HashMap<E, Object>();

	public void  set(E attribute, Object value) {
		sets.put(attribute, value);
	}

	public void reset() {
		sets = new java.util.HashMap<E, Object>();
	}

	public Class<E> eval() throws Exception {
		Class cl1 = null;
		for(E attr: sets.keySet()) {
			Class cl2 = attr.getClass();
			if(cl1 != null && cl1 != cl2)
				throw new Exception("This set clause is defined on more than one POJO class: " + cl1+ " and " + cl2);
		}

		return cl1;
	}
}
