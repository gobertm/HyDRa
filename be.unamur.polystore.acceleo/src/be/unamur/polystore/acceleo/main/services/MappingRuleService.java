package be.unamur.polystore.acceleo.main.services;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.ocl.ecore.util.OCLEcoreUtil;

import be.unamur.polystore.pml.*;
import be.unamur.polystore.scoping.PmlScopeProvider;

public class MappingRuleService {
	private static final String PATTERN_VALUE = "@VAR@";
	private static final String PATTERN_OTHER_VALUE = "@OTHERVAR@";

	public static java.util.Collection<PhysicalField> getMappedPhysicalFields(Attribute attr, MappingRules rules) {
		List<PhysicalField> res = new ArrayList<PhysicalField>();
		for (AbstractMappingRule rule : rules.getMappingRules()) {
			if (rule instanceof EntityMappingRule) {
				EntityMappingRule er = (EntityMappingRule) rule;
				for (int i = 0; i < er.getAttributesConceptual().size(); i++) {
					Attribute a = er.getAttributesConceptual().get(i);
					if (a == attr) {
						PhysicalField pf = er.getPhysicalFields().get(i);

						// PhysicalField origin = getOriginalPhysicalField(pf);
						res.add(pf);
					}
				}
			}
		}
		return res;
	}

	public static Set<Database> getConcernedDatabases(Attribute attr, Domainmodel domain) {
		Set<Database> res = new HashSet<Database>();
		java.util.Collection<PhysicalField> fields = getMappedPhysicalFields(attr, domain.getMappingRules());
		for (PhysicalField field : fields) {
			AbstractPhysicalSchema schema = getPhysicalSchema(field);
			List<Database> dbs = getAttachedDatabases(schema);
			res.addAll(dbs);
		}
		return res;
	}

	public static Set<AbstractPhysicalStructure> getConcernedPhysicalStructures(EntityType ent, Domainmodel domain) {
		Set<AbstractPhysicalStructure> res = new HashSet<AbstractPhysicalStructure>();
		for (Attribute attr : ent.getAttributes()) {
			java.util.Collection<PhysicalField> fields = getMappedPhysicalFields(attr, domain.getMappingRules());
			for (PhysicalField field : fields) {
				AbstractPhysicalStructure struct = getPhysicalStructureNotEmbeddedObject(field);
				if (struct != null)
					res.add(struct);
			}
		}
		return res;
	}

	public static Set<Database> getConcernedDatabases(AbstractPhysicalStructure struct, Domainmodel domain) {

		Set<Database> res = new HashSet<Database>();
		AbstractPhysicalSchema schema = (AbstractPhysicalSchema) getFirstAncestor(AbstractPhysicalSchema.class, struct);
		res.addAll(getAttachedDatabases(schema));
		return res;
	}

	public static Set<Database> getConcernedDatabases(EntityType ent, Domainmodel domain) {
		Set<Database> res = new HashSet<Database>();
		for (Attribute attr : ent.getAttributes()) {
			java.util.Collection<PhysicalField> fields = getMappedPhysicalFields(attr, domain.getMappingRules());
			for (PhysicalField field : fields) {
				AbstractPhysicalSchema schema = getPhysicalSchema(field);
				List<Database> dbs = getAttachedDatabases(schema);
				res.addAll(dbs);
			}
		}
		return res;
	}

	private static List<Database> getAttachedDatabases(AbstractPhysicalSchema schema) {
		List<Database> res = new ArrayList<Database>();
		if (schema != null) {
			res.addAll(schema.getDatabases());
		}
		return res;
	}

	public static PhysicalField getMappedPhysicalField(Attribute attr, AbstractPhysicalStructure struct, Database db,
			MappingRules rules) {
		List<PhysicalField> fields = (List<PhysicalField>) getMappedPhysicalFields(attr, rules);
		for (PhysicalField field : fields) {
			AbstractPhysicalStructure struct2 = getPhysicalStructureNotEmbeddedObject(field);
			if (struct == struct2) {
				AbstractPhysicalSchema schema = getPhysicalSchema(field);
				List<Database> dbs = getAttachedDatabases(schema);
				if (dbs.contains(db))
					return field;
			}
		}

		return null;
	}

	private static AbstractPhysicalSchema getPhysicalSchema(EObject obj) {
		EObject res = getFirstAncestor(AbstractPhysicalSchema.class, obj);
		if (res == null)
			return null;
		return (AbstractPhysicalSchema) res;
	}

//	private static AbstractPhysicalStructure getPhysicalStructure(EObject obj) {
//		EObject res = getFirstAncestor(AbstractPhysicalStructure.class, obj);
//		if (res == null)
//			return null;
//		return (AbstractPhysicalStructure) res;
//	}

	private static AbstractPhysicalStructure getPhysicalStructureNotEmbeddedObject(EObject obj) {
		if (obj == null)
			return null;
		if (AbstractPhysicalStructure.class.isAssignableFrom(obj.getClass())
				&& !EmbeddedObject.class.isAssignableFrom(obj.getClass()))
			return (AbstractPhysicalStructure) obj;
		return getPhysicalStructureNotEmbeddedObject(obj.eContainer());
	}

	private static EObject getFirstAncestor(final Class cl, EObject obj) {
		if (obj == null)
			return null;
		if (cl.isAssignableFrom(obj.getClass()))
			return obj;

		return getFirstAncestor(cl, obj.eContainer());
	}

	private static List<EObject> getDescendents(final Class cl, EObject obj) {
		List<EObject> res = new ArrayList<EObject>();
		if (obj != null) {
			if (cl.isAssignableFrom(obj.getClass()))
				res.add(obj);
			TreeIterator<EObject> it = obj.eAllContents();
			while (it.hasNext()) {
				EObject o = it.next();
				if (o != null) {
					if (cl.isAssignableFrom(o.getClass()))
						res.add(o);
				}
			}
		}

		return res;
	}

	public static String getPhysicalName(PhysicalField field) {
		if (field instanceof ShortField) {
			ShortField shortField = (ShortField) field;
			return shortField.getName();
		}

		if (field instanceof BracketsField) {
			EObject anc = getFirstAncestor(LongField.class, field);
			if (anc != null) {
				LongField lf = (LongField) anc;
				return lf.getPhysicalName();
			}

			BracketsField bracketsField = (BracketsField) field;
			return bracketsField.getName();
		}

		if (field instanceof EmbeddedObject) {
			EmbeddedObject embeddedField = (EmbeddedObject) field;
			return embeddedField.getName();
		}

		if (field instanceof LongField) {
			LongField longField = (LongField) field;
			return longField.getPhysicalName();
		}
		return null;
	}

	public static String getPreparedValue(Attribute attr, PhysicalField field, LongField parent,
			boolean escapeSQLReservedChar, boolean escapeMongoReservedChar) {

		if (parent instanceof LongField && field instanceof BracketsField) {
			String column = "";
			BracketsField br = (BracketsField) field;
			for (TerminalExpression expr : parent.getPattern()) {
				if (expr instanceof BracketsField) {
					BracketsField f = (BracketsField) expr;
					if (f.getName().equals(br.getName()))
						column += getPatternValue();
					else
						column += getPatternOtherValue();
				} else {
					// STRING
					column += escapeReservedChar(escapeSQLReservedChar, escapeMongoReservedChar, expr.getLiteral());
				}

			}
			return "\"" + column + "\"";

		}

		return getPatternValue();
	}

	private static String escapeReservedChar(boolean sql, boolean mongo, String str) {
		if (str == null)
			return null;
		if (sql) {
			// TODO complete the list of reserved sql char
			str = str.replaceAll("_", "\\\\\\\\_").replaceAll("%", "\\\\\\\\%");
		}

		if (mongo) {
			// TODO complete the list of reserved char in perl regex
			str = str.replaceAll("\\*", "\\\\\\\\*");
			str = str.replaceAll("\\^", "\\\\\\\\^");
			str = str.replaceAll("\\$", "\\\\\\\\\\$");
		}

		return str;

	}
	
	public static void main(String[] args) {
		System.out.println(escapeReservedChar(false, true, "$*^"));
	}

//	public static void main(String[] args) {
//		String str = "a_b_%";
//		System.out.println(escapeReservedChar(str));
//	}

	public static String getPatternValue() {
		return PATTERN_VALUE;
	}

	public static String getPatternOtherValue() {
		return PATTERN_OTHER_VALUE;
	}

}
