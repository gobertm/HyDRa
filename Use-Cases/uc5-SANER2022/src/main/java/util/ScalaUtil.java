package util;

import java.util.ArrayList;
import java.util.List;

import scala.collection.JavaConverters;

public class ScalaUtil {
	
	public static List<String> javaList(Object o) {
		if(o instanceof scala.collection.mutable.WrappedArray) {
			return new ArrayList<String>(JavaConverters.asJavaCollection(((scala.collection.mutable.WrappedArray) o).toList()));
		}
		if(o instanceof WrappedArray) {
			return ((WrappedArray) o).list();
		}
		
		return null;
	}
	
}

