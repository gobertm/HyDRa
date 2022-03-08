package be.unamur.polystore.acceleo.main.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.ocl.ecore.OCL;

import be.unamur.polystore.pml.AbstractMappingRule;
import be.unamur.polystore.pml.AbstractPhysicalSchema;
import be.unamur.polystore.pml.AbstractPhysicalStructure;
import be.unamur.polystore.pml.ArrayField;
import be.unamur.polystore.pml.Attribute;
import be.unamur.polystore.pml.BracketsField;
import be.unamur.polystore.pml.Cardinality;
import be.unamur.polystore.pml.Collection;
import be.unamur.polystore.pml.ConceptualSchema;
import be.unamur.polystore.pml.Database;
import be.unamur.polystore.pml.EmbeddedObject;
import be.unamur.polystore.pml.EntityMappingRule;
import be.unamur.polystore.pml.EntityType;
import be.unamur.polystore.pml.LongField;
import be.unamur.polystore.pml.MappingRules;
import be.unamur.polystore.pml.PhysicalField;
import be.unamur.polystore.pml.Reference;
import be.unamur.polystore.pml.RelationshipMappingRule;
import be.unamur.polystore.pml.RelationshipType;
import be.unamur.polystore.pml.ShortField;
import be.unamur.polystore.pml.TerminalExpression;

public class Util {

	public static String ARTIFACTID = "be.unamur.polystore";
	public static String GROUPID = "example";

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
				.replaceAll("\\*", "\\\\\\\\*").replaceAll("\\$", "\\\\\\\\\\$").replaceAll("\\(", "\\\\\\(")
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

	public static java.util.Collection<Attribute> getMappedAttributes(PhysicalField field, final Object entOrRel,
			final MappingRules rules) {
		if (entOrRel == null)
			return new ArrayList<Attribute>();

		if (entOrRel instanceof EntityType)
			return getMappedAttributes(field, (EntityType) entOrRel, rules);

		if (entOrRel instanceof RelationshipType)
			return getMappedAttributes(field, (RelationshipType) entOrRel, rules);

		return new ArrayList<Attribute>();

	}

	/**
	 * Recursive function if argument field is an EmbeddedObject. Goes through the
	 * mapping rules and return a list of conceptual attributes that are mapped to
	 * the given PhysicalField.
	 * 
	 * @param field
	 * @param ent
	 * @param rules
	 * @return
	 */
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

	public static java.util.Collection<Attribute> getMappedAttributes(PhysicalField field, final RelationshipType rel,
			final MappingRules rules) {
		Set<Attribute> res = new HashSet<Attribute>();

		if (field instanceof EmbeddedObject) {
			for (PhysicalField pf : ((EmbeddedObject) field).getFields())
				res.addAll(getMappedAttributes(pf, rel, rules));
		} else if (rules != null)
			for (AbstractMappingRule rule : rules.getMappingRules()) {
				
				if (rule instanceof RelationshipMappingRule) {
					RelationshipMappingRule r = (RelationshipMappingRule) rule;
					if (r.getRelationshipConceptual() == rel && r.getAttributesConceptual().size() > 0) {
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
				}			}
		return new ArrayList<Attribute>(res);
	}

	public static java.util.Collection<Attribute> getMappedAttributes(EntityType entity,
			AbstractPhysicalStructure structure, Database db, MappingRules rules) {
		Set<Attribute> res = new HashSet<Attribute>();

		for (AbstractMappingRule rule : rules.getMappingRules()) {
			if (rule instanceof EntityMappingRule) {
				EntityMappingRule r = (EntityMappingRule) rule;
				if (r.getPhysicalStructure() == structure) {
					if (r.getEntityConceptual() == entity && r.getAttributesConceptual().size() > 0) {
						List<PhysicalField> fields = r.getPhysicalFields();
						for (int i = 0; i < fields.size(); i++) {
							res.add(r.getAttributesConceptual().get(i));
						}
					}
				}
			}
		}
		return res;
	}

	public static java.util.Collection<Attribute> getMappedAttributes(RelationshipType rel,
			AbstractPhysicalStructure structure, Database db, MappingRules rules) {
		Set<Attribute> res = new HashSet<Attribute>();

		for (AbstractMappingRule rule : rules.getMappingRules()) {
			if (rule instanceof RelationshipMappingRule) {
				RelationshipMappingRule r = (RelationshipMappingRule) rule;
				if (r.getPhysicalStructure() == structure) {
					if (r.getRelationshipConceptual() == rel && r.getAttributesConceptual().size() > 0) {
						List<PhysicalField> fields = r.getPhysicalFields();
						for (int i = 0; i < fields.size(); i++) {
							res.add(r.getAttributesConceptual().get(i));
						}
					}
				}
			}
		}
		return res;
	}

	public static java.util.Collection<EmbeddedObject> getChildrenArrayPhysicalFieldsHavingDescendantMappedToGivenEntityType(
			Object arg, Object entOrRel, MappingRules rules) {
		return getChildrenArrayPhysicalFieldsHavingDescendantMappedToGivenEntityTypeOrToRefField(arg, entOrRel, rules,
				null);
	}

	public static java.util.Collection<EmbeddedObject> getChildrenArrayPhysicalFieldsHavingDescendantMappedToGivenEntityTypeOrToRefField(
			Object arg, Object entOrRel, MappingRules rules, List<PhysicalField> refFields) {
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
						java.util.Collection<Attribute> mappedAttributes = getMappedAttributes(field, entOrRel, rules);
						if (mappedAttributes.size() > 0 || isContainedInPhysicalFields(refFields, field))
							res.add((EmbeddedObject) field);
					}
				}
				if (field instanceof ArrayField) {
					java.util.Collection<Attribute> mappedAttributes = getMappedAttributes(field, entOrRel, rules);
				}
			}

		return res;
	}

	private static boolean isContainedInPhysicalFields(List<PhysicalField> refFields, PhysicalField field) {
		if (refFields != null)
			for (PhysicalField refField : refFields)
				if (isContainedInPhysicalField(refField, field))
					return true;
		return false;

	}

	private static boolean isContainedInPhysicalField(PhysicalField refField, PhysicalField field) {
		if (refField != null) {
			if (field instanceof LongField) {
				if (((LongField) field).getPattern().contains(refField))
					return true;
			}
			if (refField.equals(field))
				return true;
			if (field instanceof EmbeddedObject) {
				for (PhysicalField pf : ((EmbeddedObject) field).getFields())
					return isContainedInPhysicalField(refField, pf);
			}
			return false;
		}
		return false;
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

	public static java.util.Collection<PhysicalField> getFields(Reference ref, Boolean hasToReturnTargetFields) {
		List<PhysicalField> res = (hasToReturnTargetFields) ? ref.getTargetField() : ref.getSourceField();
		return res;
	}

	public static String getPhysicalFieldAbsolutePath(PhysicalField field) {
		return getAbsoluteName(field);

	}

	private static String getAbsoluteName(EObject o) {
		if (o instanceof AbstractPhysicalSchema)
			return ((AbstractPhysicalSchema) o).getName();

		if (o instanceof PhysicalField) {
			return getAbsoluteName(o.eContainer()) + MappingRuleService.getPhysicalName((PhysicalField) o);
		}

		if (o instanceof AbstractPhysicalStructure) {
			return getAbsoluteName(o.eContainer()) + ((AbstractPhysicalStructure) o).getName();
		}

		return null;

	}

	public static String getPomArtifactId(EObject obj) {
		EObject res = getFirstAncestor(ConceptualSchema.class, obj);
		if (res == null)
			return ARTIFACTID;
		ConceptualSchema cs = (ConceptualSchema) res;
		return cs.getName();
		
	}

	public static String getPomGroupId(EObject obj) {
		EObject res = getFirstAncestor(ConceptualSchema.class, obj);
		if (res == null)
			return GROUPID;
		ConceptualSchema cs = (ConceptualSchema) res;
		return cs.getName();
	}

	public static PhysicalField getStartField(PhysicalField field) {
		if (field.eContainer() instanceof LongField)
			return (LongField) field.eContainer();
		return field;
	}

	private static EObject getFirstAncestor(final Class cl, EObject obj) {
		if (obj == null)
			return null;
		if (cl.isAssignableFrom(obj.getClass()))
			return obj;

		return getFirstAncestor(cl, obj.eContainer());
	}

}
