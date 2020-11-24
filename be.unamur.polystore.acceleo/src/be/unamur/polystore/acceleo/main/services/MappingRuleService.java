package be.unamur.polystore.acceleo.main.services;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;

import be.unamur.polystore.pml.*;

public class MappingRuleService {
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

	private static AbstractPhysicalSchema getPhysicalSchema(EObject obj) {
		EObject res = getFirstAncestor(AbstractPhysicalSchema.class, obj);
		if (res == null)
			return null;
		return (AbstractPhysicalSchema) res;
	}

	private static EObject getFirstAncestor(final Class cl, EObject obj) {
		if (obj == null)
			return null;
		if (cl.isAssignableFrom(obj.getClass()))
			return obj;

		return getFirstAncestor(cl, obj.eContainer());
	}

}
