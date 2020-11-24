package be.unamur.polystore.acceleo.main.services;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import be.unamur.polystore.pml.*;

public class MappingRuleService {
	public static java.util.Collection<PhysicalField> getMappedPhysicalFields(Attribute attr, MappingRules rules) {
		List<PhysicalField> res = new ArrayList<PhysicalField>();
		for(AbstractMappingRule rule : rules.getMappingRules()) {
			if(rule instanceof EntityMappingRule) {
				EntityMappingRule er = (EntityMappingRule) rule;
				for(int i = 0; i < er.getAttributesConceptual().size(); i++) {
					Attribute a = er.getAttributesConceptual().get(i);
					if(a == attr) {
						res.add(er.getPhysicalFields().get(i));
					}
				}
			}
		}
		return res;
	}
	

}
