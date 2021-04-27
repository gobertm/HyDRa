package conditions;

public enum Operator {
	EQUALS, NOT_EQUALS, GT, GTE, LT, LTE, CONTAINS/**, START_WITH, END_WITH**/;
	
	public boolean evaluate(Object objectValue, Object testValue) {
			if(this == Operator.EQUALS) {
				if(testValue == null && objectValue!= null)
					return false;
				if(testValue != null && objectValue == null)
					return false;	
				return testValue.equals(objectValue);
			}
			if(this == Operator.NOT_EQUALS) {
				if(testValue == null && objectValue!= null)
					return true;
				if(testValue != null && objectValue == null)
					return true;	
				return !testValue.equals(objectValue);
			}

			if(testValue == null && objectValue == null) {
				if(this == Operator.GTE || this == Operator.LTE || this == Operator.CONTAINS)
					return true;
				return false;
			}
			
			if(objectValue == null)
				return false;

			if(this == Operator.GT) {
				return ((Comparable) testValue).compareTo(objectValue) < 0;
			}
			if(this == Operator.GTE) {
				return ((Comparable) testValue).compareTo(objectValue) <= 0;
			}
			if(this == Operator.LT) {
				return ((Comparable) testValue).compareTo(objectValue) > 0;
			}
			if(this == Operator.LTE)
				return ((Comparable) testValue).compareTo(objectValue) >= 0;
			if(this == Operator.CONTAINS) {
				if(objectValue == null)
					return false;
				return objectValue.toString().contains(testValue.toString());
			}
			
			return false;
	}

	public String getSQLOperator() {
		if(this == EQUALS)
			return "=";
		if(this == NOT_EQUALS)
			return "<>";
		if(this == GT)
			return ">";
		if(this == GTE)
			return ">=";
		if(this == LT)
			return "<";
		if(this == LTE)
			return "<=";
		if(this == CONTAINS)
			return "LIKE";
		/**if(this == START_WITH)
			return "LIKE";
		if(this == END_WITH)
			return "LIKE";**/
		
		return null;
	}

	public String getMongoDBOperator() {
		if(this == EQUALS)
			return "$eq";
		if(this == NOT_EQUALS)
			return "$ne";
		if(this == GT)
			return "$gt";
		if(this == GTE)
			return "$gte";
		if(this == LT)
			return "$lt";
		if(this == LTE)
			return "$lte";
		if(this == CONTAINS)
			return "$regex";
		/**if(this == START_WITH)
			return "LIKE";
		if(this == END_WITH)
			return "LIKE";**/
		
		return null;

		
	}

}
