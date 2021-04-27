package util;

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

}
