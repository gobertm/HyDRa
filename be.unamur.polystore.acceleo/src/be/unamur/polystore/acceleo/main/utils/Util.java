package be.unamur.polystore.acceleo.main.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.ocl.ecore.OCL;

import be.unamur.polystore.pml.AbstractMappingRule;
import be.unamur.polystore.pml.AbstractPhysicalStructure;
import be.unamur.polystore.pml.Attribute;
import be.unamur.polystore.pml.BracketsField;
import be.unamur.polystore.pml.Cardinality;
import be.unamur.polystore.pml.Collection;
import be.unamur.polystore.pml.EmbeddedObject;
import be.unamur.polystore.pml.EntityMappingRule;
import be.unamur.polystore.pml.EntityType;
import be.unamur.polystore.pml.LongField;
import be.unamur.polystore.pml.MappingRules;
import be.unamur.polystore.pml.PhysicalField;
import be.unamur.polystore.pml.ShortField;
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
				.replaceAll("\\*", "\\\\\\\\*").replaceAll("\\$", "\\\\\\\\\\$").replaceAll("\\(", "\\\\\\(").replaceAll("\\)", "\\\\\\)"); // TODO
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

	public static java.util.Collection<PhysicalField> getChildrenNonArrayPhysicalFields(Object arg) {
		List<PhysicalField> res = new ArrayList<PhysicalField>();

		List<PhysicalField> list = null;

		if (arg instanceof Collection) {
			Collection coll = (Collection) arg;
			list = coll.getFields();

		} else if (arg instanceof EmbeddedObject) {
			EmbeddedObject emb = (EmbeddedObject) arg;
			list = emb.getFields();
		} else if (arg instanceof LongField) {
			LongField lf = (LongField) arg;
			list = new ArrayList<PhysicalField>();
			for (TerminalExpression expr : lf.getPattern())
				if (expr instanceof PhysicalField)
					list.add((PhysicalField) expr);

		}

		if (list != null)
			for (PhysicalField field : list) {
				if (field instanceof EmbeddedObject) {
					EmbeddedObject o = (EmbeddedObject) field;
					if (o.getCardinality() == Cardinality.ONE || o.getCardinality() == Cardinality.ZERO_ONE) {
						res.addAll(getChildrenNonArrayPhysicalFields(o));
					}
				} else {
					if (field instanceof LongField) {
						res.addAll(getChildrenNonArrayPhysicalFields(field));
					} else {
						// shortField or bracketField
						res.add(field);
					}
				}

			}

		return res;

	}

	public static java.util.Collection<Attribute> getMappedAttributes(PhysicalField field, final EntityType ent,
			final MappingRules rules) {
		Set<Attribute> res = new HashSet<Attribute>();

		if (field instanceof EmbeddedObject) {
			for (PhysicalField pf : ((EmbeddedObject) field).getFields())
				res.addAll(getMappedAttributes(pf, ent, rules));
		} else if (rules != null)
			for (AbstractMappingRule rule : rules.getMappingRules()) {
				if (rule instanceof EntityMappingRule) {
					EntityMappingRule r = (EntityMappingRule) rule;
					if (r.getEntityConceptual() == ent && r.getAttributesConceptual().size() > 0) {
						List<PhysicalField> fields = r.getPhysicalFields();
						for (int i = 0; i < fields.size(); i++) {
							PhysicalField pf = fields.get(i);
							if (field instanceof LongField) {
								if (((LongField) field).getPattern().contains(pf))
									res.add(r.getAttributesConceptual().get(i));

							} else if (pf == field)
								res.add(r.getAttributesConceptual().get(i));
						}
					}
				}
			}
		return new ArrayList<Attribute>(res);
	}

	public static java.util.Collection<EmbeddedObject> getChildrenArrayPhysicalFieldsHavingDescendantMappedToGivenEntityType(
			Object arg, EntityType ent, MappingRules rules) {
		List<EmbeddedObject> res = new ArrayList<EmbeddedObject>();
		List<PhysicalField> list = null;
		if (arg instanceof Collection) {
			Collection coll = (Collection) arg;
			list = coll.getFields();

		} else if (arg instanceof EmbeddedObject) {
			EmbeddedObject emb = (EmbeddedObject) arg;
			list = emb.getFields();
		}

		if (list != null)
			for (PhysicalField field : list) {
				if (field instanceof EmbeddedObject) {
					if (((EmbeddedObject) field).getCardinality() == Cardinality.ONE_MANY
							|| ((EmbeddedObject) field).getCardinality() == Cardinality.ZERO_MANY) {
						java.util.Collection<Attribute> mappedAttributes = getMappedAttributes(field, ent, rules);
						if (mappedAttributes.size() > 0)
							res.add((EmbeddedObject) field);
					}
				}
			}

		return res;
	}

	public static java.util.Collection<EmbeddedObject> getSequenceOfNonArrayEmbeddedObjects(EObject field) {
		List<EmbeddedObject> res = new ArrayList<EmbeddedObject>();
		if (field != null) {
			if (field instanceof EmbeddedObject && (((EmbeddedObject) field).getCardinality() == Cardinality.ONE
					|| ((EmbeddedObject) field).getCardinality() == Cardinality.ZERO_ONE))
				res.add((EmbeddedObject) field);
			res.addAll(getSequenceOfNonArrayEmbeddedObjects(field.eContainer()));
		}

		return res;
	}

}
