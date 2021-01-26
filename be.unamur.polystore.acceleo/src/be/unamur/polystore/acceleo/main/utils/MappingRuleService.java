package be.unamur.polystore.acceleo.main.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.ocl.ecore.OCL;
import org.eclipse.ocl.ecore.util.OCLEcoreUtil;

import be.unamur.polystore.pml.*;
import be.unamur.polystore.scoping.PmlScopeProvider;

public class MappingRuleService {
	private static final String PATTERN_VALUE = "@VAR@";
	private static final String PATTERN_OTHER_VALUE = "@OTHERVAR@";

	/**
	 * Gives a collection of corresponding mapped Physical Field in the pml model of
	 * the given attribute, reading the EntityMappingRule rules.
	 * 
	 * @param attr  the conceptual attribute we look for
	 * @param rules The mapping rules in pml Model
	 * @return A collection of Physical Field
	 */
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

	/**
	 * Given a specific conceptual attribute and a structure
	 * AbstractPhysicalStructure and a Database. Returns the first mapped
	 * PhysicalField found.
	 * 
	 * @param attr
	 * @param struct
	 * @param db
	 * @param rules
	 * @return
	 */
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
	
	public static List<Reference> getRefOfPhysicalStructure(AbstractPhysicalStructure structure) {
		List<Reference> res = new ArrayList<Reference>();
		if(structure instanceof Table) {
			res.addAll(((Table)structure).getReferences());
		}
		if(structure instanceof Collection)
			res.addAll(((Collection)structure).getReferences());
		if(structure instanceof TableColumnDB)
			res.addAll(((TableColumnDB)structure).getReferences());
		// TODO For graph and others
		return res;
	}

	/**
	 * Returns a Set of Database (PML domain object) that contains the mapped
	 * Physiucal Fields of the given conceptual attribute Note : Uses @method
	 * getMappedPhysicalFields(Attribute a, MappingRules rules),
	 * getPhysicalSchema(field) , getAttachedDatabases(schema);
	 * 
	 * @param attr   The conceputal attribute
	 * @param domain
	 * @return A Set of Database
	 */
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

	/**
	 * Given an EntityType , returns a Set of all the mapped PhysicalStructure ,
	 * except the EmbeddedObject type. Note : uses getMappedPhysicalFields,
	 * getPhysicalStructureNotEmbeddedObject
	 * 
	 * @param ent
	 * @param domain
	 * @return
	 */
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

	/**
	 * Given an AbstractPhysicalStructure, retrieves its schema and returns the
	 * mapped databases using a call to the getAttachedDatabase(schema)
	 * 
	 * @param struct
	 * @param domain
	 * @return
	 */
	public static Set<Database> getConcernedDatabases(AbstractPhysicalStructure struct, Domainmodel domain) {

		Set<Database> res = new HashSet<Database>();
		AbstractPhysicalSchema schema = (AbstractPhysicalSchema) getFirstAncestor(AbstractPhysicalSchema.class, struct);
		res.addAll(getAttachedDatabases(schema));
		return res;
	}

	/**
	 * Gets the Databases based onn EntityType. Goes through the mapped
	 * PhysicalField of this entity, gets its AbstractPhysicalSchema and returns the
	 * Database using getAttachedDatabases
	 * 
	 * @param ent
	 * @param domain
	 * @return
	 */
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

	/**
	 * Returns the Databases of an AbstractPhysicalShcema
	 * 
	 * @param schema
	 * @return
	 */
	private static List<Database> getAttachedDatabases(AbstractPhysicalSchema schema) {
		List<Database> res = new ArrayList<Database>();
		if (schema != null) {
			res.addAll(schema.getDatabases());
		}
		return res;
	}

	public static AbstractPhysicalSchema getPhysicalSchema(EObject obj) {
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
				&& !EmbeddedObject.class.isAssignableFrom(obj.getClass())
				&& !KVComplexField.class.isAssignableFrom(obj.getClass()))
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

	public static Set<Reference> getMappedReferences(Role role, MappingRules rules) {
		Set<Reference> res = new HashSet<Reference>();
		for (AbstractMappingRule rule : rules.getMappingRules())
			if (rule instanceof RoleToReferenceMappingRule) {
				if (((RoleToReferenceMappingRule) rule).getRoleConceptual() == role)
					res.add(((RoleToReferenceMappingRule) rule).getReference());
			}
		return res;
	}

}
