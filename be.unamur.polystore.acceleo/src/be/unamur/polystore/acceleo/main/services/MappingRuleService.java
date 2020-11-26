package be.unamur.polystore.acceleo.main.services;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.ocl.ecore.util.OCLEcoreUtil;

import be.unamur.polystore.pml.*;

public class MappingRuleService {
	private static final String SQL_PATTERN_VALUE = "$VAR$";
	
	public static java.util.Collection<PhysicalField> getMappedPhysicalFields(Attribute attr, MappingRules rules) {
		List<PhysicalField> res = new ArrayList<PhysicalField>();
		for (AbstractMappingRule rule : rules.getMappingRules()) {
			if (rule instanceof EntityMappingRule) {
				EntityMappingRule er = (EntityMappingRule) rule;
				for (int i = 0; i < er.getAttributesConceptual().size(); i++) {
					Attribute a = er.getAttributesConceptual().get(i);
					if (a == attr) {
						res.add(er.getPhysicalFields().get(i));
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
				AbstractPhysicalStructure struct = getPhysicalStructure(field);
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
	
	public static PhysicalField getMappedPhysicalField(Attribute attr, Database db, MappingRules rules) {
		List<PhysicalField> fields = (List<PhysicalField>) getMappedPhysicalFields(attr, rules);
		for(PhysicalField field: fields) {
			AbstractPhysicalSchema schema = getPhysicalSchema(field);
			List<Database> dbs = getAttachedDatabases(schema);
			if(dbs.contains(db))
				return field;
		}
		
		return null;
	}

	private static AbstractPhysicalSchema getPhysicalSchema(EObject obj) {
		EObject res = getFirstAncestor(AbstractPhysicalSchema.class, obj);
		if (res == null)
			return null;
		return (AbstractPhysicalSchema) res;
	}
	
	private static AbstractPhysicalStructure getPhysicalStructure(EObject obj) {
		EObject res = getFirstAncestor(AbstractPhysicalStructure.class, obj);
		if (res == null)
			return null;
		return (AbstractPhysicalStructure) res;
	}

	private static EObject getFirstAncestor(final Class cl, EObject obj) {
		if (obj == null)
			return null;
		if (cl.isAssignableFrom(obj.getClass()))
			return obj;

		return getFirstAncestor(cl, obj.eContainer());
	}

	public static String getPhysicalName(PhysicalField field) {
		if(field instanceof ShortField) {
			ShortField shortField = (ShortField) field;
			return shortField.getName();
		}
		
		if(field instanceof BracketsField) {
			BracketsField bracketsField = (BracketsField) field;
			return bracketsField.getName();
		}
		
		if(field instanceof EmbeddedObject) {
			EmbeddedObject embeddedField = (EmbeddedObject) field;
			return embeddedField.getName();
		}
		
		if(field instanceof LongField) {
			LongField longField = (LongField) field;
			return longField.getPhysicalName();
		}
		return null;
	}
	
	public static String getSQLPreparedValue(Attribute attr, PhysicalField field) {
		
		
		if(field instanceof LongField) {
			LongField longField = (LongField) field;
			
		}
		
		return getSQLPatternValue();
	}
	
	public static String getSQLPatternValue() {
		return SQL_PATTERN_VALUE;
	}
	
}
