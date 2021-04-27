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
		if(obj instanceof Actor)
			return evaluateActor((Actor) obj);
		if(obj instanceof Director)
			return evaluateDirector((Director) obj);
		if(obj instanceof Movie)
			return evaluateMovie((Movie) obj);
		return true;
	}


	private boolean evaluateActor(Actor obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		ActorAttribute attr = (ActorAttribute) this.attribute;
		Object objectValue = null;

		if(attr == ActorAttribute.id)
			objectValue = obj.getId();
		if(attr == ActorAttribute.fullName)
			objectValue = obj.getFullName();
		if(attr == ActorAttribute.yearOfBirth)
			objectValue = obj.getYearOfBirth();
		if(attr == ActorAttribute.yearOfDeath)
			objectValue = obj.getYearOfDeath();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateDirector(Director obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		DirectorAttribute attr = (DirectorAttribute) this.attribute;
		Object objectValue = null;

		if(attr == DirectorAttribute.id)
			objectValue = obj.getId();
		if(attr == DirectorAttribute.firstName)
			objectValue = obj.getFirstName();
		if(attr == DirectorAttribute.lastName)
			objectValue = obj.getLastName();
		if(attr == DirectorAttribute.yearOfBirth)
			objectValue = obj.getYearOfBirth();
		if(attr == DirectorAttribute.yearOfDeath)
			objectValue = obj.getYearOfDeath();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateMovie(Movie obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		MovieAttribute attr = (MovieAttribute) this.attribute;
		Object objectValue = null;

		if(attr == MovieAttribute.id)
			objectValue = obj.getId();
		if(attr == MovieAttribute.primaryTitle)
			objectValue = obj.getPrimaryTitle();
		if(attr == MovieAttribute.originalTitle)
			objectValue = obj.getOriginalTitle();
		if(attr == MovieAttribute.isAdult)
			objectValue = obj.getIsAdult();
		if(attr == MovieAttribute.startYear)
			objectValue = obj.getStartYear();
		if(attr == MovieAttribute.runtimeMinutes)
			objectValue = obj.getRuntimeMinutes();
		if(attr == MovieAttribute.averageRating)
			objectValue = obj.getAverageRating();
		if(attr == MovieAttribute.numVotes)
			objectValue = obj.getNumVotes();

		return operator.evaluate(objectValue, this.getValue());
	}

	
}
