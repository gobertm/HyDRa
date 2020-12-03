package be.unamur.polystore.acceleo.main.services;

import be.unamur.polystore.pml.BracketsField;
import be.unamur.polystore.pml.LongField;
import be.unamur.polystore.pml.PhysicalField;
import be.unamur.polystore.pml.TerminalExpression;

public class Util {

	public static String getJavaRegexFromLongField(LongField field) {
		if (field == null)
			return null;

		String res = "";
		for (TerminalExpression expr : field.getPattern()) {
			if (expr instanceof BracketsField) {
				res += "(.*)";
			} else {
				String literal = expr.getLiteral();
				literal = escapeReservedCharsInJavaRegex(literal);
				res += "(" + literal + ")";
			}
		}

		return res;
	}

	private static String escapeReservedCharsInJavaRegex(String literal) {
		if (literal == null)
			return null;
		return literal/** .replaceAll("\s", "\\\\s") **/
				.replaceAll("\\$", "\\\\\\$")
				.replaceAll("\\(", "\\\\\\(")
				.replaceAll("\\)", "\\\\\\)"); // TODO
	}

	public static String getPositionInLongField(PhysicalField field, LongField parent) {
		if (field instanceof BracketsField) {
			BracketsField b = (BracketsField) field;
			int index = 1;
			for (TerminalExpression expr : parent.getPattern()) {
				if (expr instanceof BracketsField) {
					BracketsField b2 = (BracketsField) expr;
					if (b.getName().equals(b2.getName()))
						return "" + index;
				}
				index++;
			}
		}

		return null;
	}

}
