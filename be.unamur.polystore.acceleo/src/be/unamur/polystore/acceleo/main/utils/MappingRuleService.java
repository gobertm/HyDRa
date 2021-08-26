package be.unamur.polystore.acceleo.main.utils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.management.relation.Relation;

import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.EMap;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.ocl.ecore.OCL;
import org.eclipse.ocl.ecore.util.OCLEcoreUtil;

import be.unamur.polystore.pml.*;
import be.unamur.polystore.scoping.PmlScopeProvider;

/**
 * @author Maxime Gobert
 *
 */
/**
 * @author Maxime Gobert
 *
 */
/**
 * @author Maxime Gobert
 *
 */
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
	
	public static Set<AbstractPhysicalStructure> getMappedPhysicalStructureOfRole(Role role, MappingRules rules) {
		Set<AbstractPhysicalStructure> res = new HashSet<>();
		for (AbstractMappingRule rule : rules.getMappingRules()) {
			if (rule instanceof RoleToReferenceMappingRule) {
				RoleToReferenceMappingRule roleMappingRule = (RoleToReferenceMappingRule) rule;
				if(roleMappingRule.getRoleConceptual().equals(role)) {
					res.add(getPhysicalStructureNotEmbeddedObject(roleMappingRule.getReference()));
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
	
	
	/** Returns concerned physical structure (physical structure of field mapped to attribute of given entity type) that are not Standalone structure of ent , nor ComplexEmbedded (2 cascading role embeddeded).
	 * Also returns structures of mapped mandatory role of Ent 
	 * @param ent
	 * @param domain
	 * @return
	 */
	public static Set<AbstractPhysicalStructure> getComplexPhysicalStructures(EntityType ent, Domainmodel domain) {
		Set<AbstractPhysicalStructure> res = new HashSet<AbstractPhysicalStructure>();
		for (Attribute attr : ent.getAttributes()) {
			java.util.Collection<PhysicalField> fields = getMappedPhysicalFields(attr, domain.getMappingRules());
			for (PhysicalField field : fields) {
				AbstractPhysicalStructure struct = getPhysicalStructureNotEmbeddedObject(field);
				if (struct != null)
					res.add(struct);
			}
		}
		res.removeAll(getMappedComplexEmbeddedStructureOfEntity(ent, domain));
		res.removeAll(getMappedPhysicalStructureToInsertSingleE(ent, domain));
		res.addAll(getStructureOfMappedRole(ent, domain));
		return res;
	}
	
	public static Set<AbstractPhysicalStructure> getStructureOfMappedRole(EntityType ent, Domainmodel model){
		Set<AbstractPhysicalStructure> res = new HashSet<AbstractPhysicalStructure>();
		for(RelationshipType rel : model.getConceptualSchema().getRelationships()) {
			for(Role role : rel.getRoles()) {
				if(role.getEntity().equals(ent) && (role.getCardinality()==Cardinality.ONE || role.getCardinality()==Cardinality.ONE_MANY)) {
					res.addAll(getMappedPhysicalStructureOfRole(role,model.getMappingRules()));
				}
			}
		}
		return res;
	}
	
	
	
	
	/** Returns PhysicalStructures where the given entity is mapped to the first level fields and where there exists cascading mandatory embedded structures mapped to roles. (Identifies structures where we need POJO attribute set.) 
	 * @param entity
	 * @param domain
	 * @return
	 */
	public static Set<AbstractPhysicalStructure> getMappedComplexEmbeddedStructureOfEntity(EntityType entity,Domainmodel domain) {
		Set<AbstractPhysicalStructure> res = new HashSet();
		List<PhysicalField> fields = new ArrayList<>();
		boolean firstlevelFieldMappedToEntity=false;
		for(AbstractPhysicalStructure struct : getConcernedPhysicalStructures(entity, domain)) {
			if(struct instanceof Collection) {
				fields = ((Collection) struct).getFields();
				for(PhysicalField field : fields) {
					if(isMappedToEntity(field, entity, domain.getMappingRules()))
						firstlevelFieldMappedToEntity=true;
				}
				for(PhysicalField field : fields) {
					if(field instanceof EmbeddedObject) {
						if(firstlevelFieldMappedToEntity 
								&& isMappedToRole(field, domain.getMappingRules()) 
								&& (((EmbeddedObject)field).getCardinality().equals(Cardinality.ONE) || ((EmbeddedObject)field).getCardinality().equals(Cardinality.ONE_MANY))) {
							List<EObject> embeddedDescendents = getDescendents(EmbeddedObject.class, field);
							if(embeddedDescendents.size()==0)
								break;
							else {
								for(EObject e : embeddedDescendents) {
									if(isMappedToRole(e, domain.getMappingRules())
											&& (((EmbeddedObject)e).getCardinality().equals(Cardinality.ONE) || ((EmbeddedObject)e).getCardinality().equals(Cardinality.ONE_MANY))) {
										res.add(struct);
										break;
									}
								}
							}
						}
					}
				}
			}
		}
		return res;
	}

	
	/**
	 * Returns standalone structures where we can insert 'ent'.
	 * From all the mapped structure. We check that it is not contained in an Embedded Structure mapped to a role. If yes we remove this structure.
	 * In Key Value we check that key elements are not mapped to another entity type or a role. 
	 * @param ent
	 * @param domain
	 * @return
	 */
	public static List<AbstractPhysicalStructure> getMappedPhysicalStructureToInsertSingleE(EntityType ent, Domainmodel domain){
		List<AbstractPhysicalStructure> res ;
		res = new ArrayList<>(getConcernedPhysicalStructures(ent, domain));
		AbstractPhysicalStructure struct;
		for(int i = 0; i< res.size(); i++) {
			struct = res.get(i);
			if(struct instanceof KeyValuePair)
				continue;
			boolean flag = false;
			//Detect if the embedded objects are of cardinality 1 and mapped to a role. 
			if(struct instanceof Collection) {
				for(PhysicalField f : ((Collection)struct).getFields()) {
					if(f instanceof EmbeddedObject) {
						if(isMappedToRole(f, domain.getMappingRules())
								&& (((EmbeddedObject) f).getCardinality()==Cardinality.ONE ||((EmbeddedObject) f).getCardinality()==Cardinality.ONE_MANY))
							flag=true;
					}
				}
				if(flag) {
					res.remove(i);
					i--;
					continue;
				}
			}
			flag=false;
			for (Attribute attr : ent.getAttributes()) {
				java.util.Collection<PhysicalField> fields = getMappedPhysicalFields(attr, domain.getMappingRules());
				for (PhysicalField field : fields) {
					if(getPhysicalStructureNotEmbeddedObject(field) == struct) {
						List<EObject> embeddedParent = getAscendents(EmbeddedObject.class, field);
						if(embeddedParent.size() == 0)
							flag = true;
						else {
							flag = true;
							for(EObject e : embeddedParent) {
								if(isMappedToRole(e, domain.getMappingRules())) {
									flag=false;
									break;
								}
							}
						}
						if(flag)
							break;
					}
				}
				if(flag)
					break;
			}
			if(!flag) {
				res.remove(i);
				i--;
			}
		}

		for (Attribute attr : ent.getAttributes()) {
			java.util.Collection<PhysicalField> fields = getMappedPhysicalFields(attr, domain.getMappingRules());
			for (PhysicalField field : fields) {
				// check if field is involved in a key with other component mapped to a role
				KeyValuePair kvpair = (KeyValuePair)getFirstAncestor(KeyValuePair.class, field);
				if(kvpair!=null) {
					for(PhysicalField f : getPhysicalFieldsFromKey(kvpair.getKey())) {
						if(isMappedToRole(f, domain.getMappingRules())) {
							res.remove(getPhysicalStructureNotEmbeddedObject(f));
						}
						if(!isMappedToEntity(f, ent, domain.getMappingRules())) {
							res.remove(getPhysicalStructureNotEmbeddedObject(f));
						}
					}

				}
			}
		}
		return res;
	}

	public static EList<PhysicalField> getPhysicalFieldsFromKey(Key key){
		EList<PhysicalField> fields = new BasicEList<PhysicalField>();
		for(TerminalExpression terminal : key.getPattern()) {
			if(terminal instanceof BracketsField)
				fields.add((BracketsField)terminal);
		}
		return fields;
	}

	public static boolean isMappedToRole(EObject e, MappingRules rules) {
		for(AbstractMappingRule rule : rules.getMappingRules()) {
			if(rule instanceof RoleToEmbbededObjectMappingRule) {
				if(((RoleToEmbbededObjectMappingRule) rule).getPhysicalStructure().equals(e))
					return true;
			}
			if(rule instanceof RoleToKeyBracketsFieldMappingRule) {
				if((((RoleToKeyBracketsFieldMappingRule)rule).getPhysicalStructure().equals(e)))
					return true;
			}

		}
		return false;
	}
	
	public static boolean isMappedToEntity(EObject e, EntityType ent, MappingRules rules) {
		for(AbstractMappingRule rule : rules.getMappingRules()) {
			if(rule instanceof EntityMappingRule) {
				if(((EntityMappingRule) rule).getPhysicalFields().contains(e) && ((EntityMappingRule) rule).getEntityConceptual().equals(ent))
					return true;
			}
		}
		return false;
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

	/**
	 * Recursively goes up from the given object in order to return the PhysicalStructure which is not an EmbeddedStructure (EmbeddedObject or KVComplexField) 
	 * @param obj
	 * @return
	 */
	public static AbstractPhysicalStructure getPhysicalStructureNotEmbeddedObject(EObject obj) {
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

	public static List<EObject> getDescendents(final Class cl, EObject obj) {
		List<EObject> res = new ArrayList<EObject>();
		if (obj != null) {
			if (cl.isAssignableFrom(obj.getClass()))
				res.add(obj);
			TreeIterator<EObject> it = obj.eAllContents();
			while (it.hasNext()) {
				EObject o = it.next();
				if (o != null) {
					res.addAll(getDescendents(cl, o));
				}
			}
		}

		return res;
	}

	public static List<LongField> getDescendentsLongField(EObject obj){
		List<LongField> res = new ArrayList<>();
		for (EObject o : getDescendents(LongField.class, obj)) {
			res.add((LongField)o);
		}
		return res;
	}

	private static List<EObject> getAscendents(final Class cl, EObject obj) {
		List<EObject> res = new ArrayList<EObject>();
		EObject object = getFirstAncestor(cl, obj.eContainer());
		if(object!=null) {
			res.add(object);
			res.addAll(getAscendents(cl, object));
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

	public static Set<EmbeddedObject> getMappedEmbeddedObjects(Role role, MappingRules rules) {
		Set<EmbeddedObject> res = new HashSet<EmbeddedObject>();
		for (AbstractMappingRule rule : rules.getMappingRules())
			if (rule instanceof RoleToEmbbededObjectMappingRule) {
				if (((RoleToEmbbededObjectMappingRule) rule).getRoleConceptual() == role) {
					res.add(((RoleToEmbbededObjectMappingRule) rule).getPhysicalStructure());
				}
			}
		return res;
	}

	public static Set<AbstractPhysicalStructure> getMappedStructuresNotConcernedByRelationshipType(EntityType ent, RelationshipType rel, MappingRules rules) {
		Set<AbstractPhysicalStructure> res = new HashSet<AbstractPhysicalStructure>();

		Set<AbstractPhysicalStructure> exclusion = new HashSet<AbstractPhysicalStructure>();
		for(Role role : rel.getRoles()) {
			for(EmbeddedObject o : getMappedEmbeddedObjects(role, rules))
				exclusion.add(getPhysicalStructureNotEmbeddedObject(o));
			for(Reference ref : getMappedReferences(role, rules)) {
				exclusion.add(getPhysicalStructureNotEmbeddedObject(ref.getSourceField().get(0)));
				exclusion.add(getPhysicalStructureNotEmbeddedObject(ref.getTargetField().get(0)));
			}
		}

		for(Attribute attr : ent.getAttributes())
			for(PhysicalField field : getMappedPhysicalFields(attr, rules)) {
				AbstractPhysicalStructure struct = getPhysicalStructureNotEmbeddedObject(field);
				if(!exclusion.contains(struct))
					res.add(struct);
			}


		return res;
	}

}
