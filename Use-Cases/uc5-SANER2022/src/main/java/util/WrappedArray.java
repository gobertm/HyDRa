package util;

import java.util.List;

import org.bson.Document;

public class WrappedArray {
	private List list;
	
	public WrappedArray(List list) {
		this.list = list;
	}
	
	public int size() {
		return list.size();
	}
	
	public Object apply(int index) {
		Object o = list.get(index);
		if(o instanceof Document)
			return new Row((Document) o);
		return o;
	}

	public List list() {
		return list;
	}
	

}

