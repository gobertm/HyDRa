/*
 * generated by Xtext 2.23.0
 */
package be.unamur.polystore.validation;

import org.eclipse.xtext.EcoreUtil2;
import org.eclipse.xtext.validation.Check;

import be.unamur.polystore.pml.AbstractMappingRule;
import be.unamur.polystore.pml.ArrayField;
import be.unamur.polystore.pml.Attribute;
import be.unamur.polystore.pml.BracketsField;
import be.unamur.polystore.pml.Domainmodel;
import be.unamur.polystore.pml.Edge;
import be.unamur.polystore.pml.EntityMappingRule;
import be.unamur.polystore.pml.EntityType;
import be.unamur.polystore.pml.Identifier;
import be.unamur.polystore.pml.MappingRules;
import be.unamur.polystore.pml.PhysicalField;
import be.unamur.polystore.pml.PmlPackage;
import be.unamur.polystore.pml.Reference;
import be.unamur.polystore.pml.RelationshipMappingRule;
import be.unamur.polystore.pml.RelationshipType;
import be.unamur.polystore.pml.RoleToKeyBracketsFieldMappingRule;
import be.unamur.polystore.pml.RoleToReferenceMappingRule;
import be.unamur.polystore.pml.ShortField;

/**
 * This class contains custom validation rules. 
 *
 * See https://www.eclipse.org/Xtext/documentation/303_runtime_concepts.html#validation
 */
public class PmlValidator extends AbstractPmlValidator {
	
	public static final String INVALID_NAME = "invalidName";
	public static final String INVALID_ERULE = "invalidEntityMappingRule";

	@Check
	public void checkEntityStartsWithCapital(EntityType entity) {
		if (!Character.isUpperCase(entity.getName().charAt(0))) {
			warning("Name should start with a capital",
					PmlPackage.Literals.ENTITY_TYPE__NAME,
					INVALID_NAME);
		}
	}
	
	@Check
	public void checkRelationshipStartsWithLowerCase(RelationshipType relationship) {
		if (!Character.isLowerCase(relationship.getName().charAt(0))) {
			warning("Relationhsip names should start with a lower case letter",
					PmlPackage.Literals.RELATIONSHIP_TYPE__NAME,
					INVALID_NAME);
		}
	}
	
	@Check
	public void checkAttributeStartsWithLowerCase(Attribute attribute) {
		if (!Character.isLowerCase(attribute.getName().charAt(0))) {
			error("Attribute names must start with a lower case letter",
					PmlPackage.Literals.ATTRIBUTE__NAME,
					INVALID_NAME);
		}
	}
	
	@Check void checkCapitalLettersForEdge(Edge edge) {
		for(char c : edge.getName().toCharArray()) {
			if(!Character.isUpperCase(c) && Character.isAlphabetic(c))
				warning("Edges name should be full capital letters", PmlPackage.Literals.ABSTRACT_PHYSICAL_STRUCTURE__NAME,INVALID_NAME);
		}
	}
	
	@Check void checkAttributeVsFieldInEntityMappingRule(EntityMappingRule rule) {
		if(rule.getAttributesConceptual().size()!= rule.getPhysicalFields().size()) {
			error("Number of conceptual attributes and physical fields in entity mapping rule should be equal", PmlPackage.Literals.ENTITY_MAPPING_RULE__ENTITY_CONCEPTUAL, INVALID_ERULE);
			error("Number of conceptual attributes and physical fields in entity mapping rule should be equal", PmlPackage.Literals.ENTITY_MAPPING_RULE__PHYSICAL_STRUCTURE, INVALID_ERULE);
		}
	}
	
	@Check void checkBracketsFieldIsMapped(BracketsField field) {
		boolean mapped = false;
		MappingRules rules = EcoreUtil2.getContainerOfType(field, Domainmodel.class).getMappingRules();
		for(AbstractMappingRule rule : rules.getMappingRules()) {
			if(rule instanceof EntityMappingRule) {
				if(((EntityMappingRule) rule).getPhysicalFields().contains(field)) {
					mapped = true;
					break;
				}
			}
			if(rule instanceof RoleToReferenceMappingRule) {
				if(((RoleToReferenceMappingRule) rule).getReference().getSourceField().contains(field)) {
					mapped=true;
					break;
				}
			}
			if(rule instanceof RelationshipMappingRule) {
				if(((RelationshipMappingRule) rule).getPhysicalFields().contains(field)) {
					mapped=true;
					break;
				}
			}
			if(rule instanceof RoleToKeyBracketsFieldMappingRule) {
				if(((RoleToKeyBracketsFieldMappingRule) rule).getKeyField().equals(field)) {
					mapped=true;
					break;
				}
			}
		}
		if(!mapped) {
			warning("This physical field is not mapped in mapping rules section nor in references", PmlPackage.Literals.BRACKETS_FIELD__NAME, INVALID_NAME);
		}
	}
	
	@Check void checkShortFieldIsMapped(ShortField field) {
		boolean mapped = false;
		MappingRules rules = EcoreUtil2.getContainerOfType(field, Domainmodel.class).getMappingRules();
		for(AbstractMappingRule rule : rules.getMappingRules()) {
			if(rule instanceof EntityMappingRule) {
				if(((EntityMappingRule) rule).getPhysicalFields().contains(field)) {
					mapped = true;
					break;
				}
			}
			if(rule instanceof RoleToReferenceMappingRule) {
				if(((RoleToReferenceMappingRule) rule).getReference().getSourceField().contains(field)) {
					mapped=true;
					break;
				}
			}
			if(rule instanceof RelationshipMappingRule) {
				if(((RelationshipMappingRule) rule).getPhysicalFields().contains(field)) {
					mapped=true;
					break;
				}
			}
		}
		if(!mapped) {
			warning("This physical field is not mapped to anything in mapping rules section nor in references", PmlPackage.Literals.SHORT_FIELD__NAME, INVALID_NAME);
		}
	}
	
	@Check void checkReferenceIsMapped(Reference reference) {
		boolean mapped = false;
		MappingRules rules = EcoreUtil2.getContainerOfType(reference, Domainmodel.class).getMappingRules();
		for(AbstractMappingRule rule : rules.getMappingRules()) {
			if(rule instanceof RoleToReferenceMappingRule) {
				if(((RoleToReferenceMappingRule) rule).getReference().equals(reference)) {
					mapped=true;
				}
			}
		}
		if(!mapped) {
			warning("This reference is not mapped to anything in mapping rules section", PmlPackage.Literals.REFERENCE__NAME, INVALID_NAME);
		}

	}
	
	@Check void checkUpperCaseIdentifier(Identifier identifier) {
		for(Attribute attr : identifier.getAttributes()) {
			if(Character.isUpperCase(attr.getName().charAt(0))) {
				error("Attributes in identifiers cannot start with uppercase letter. Causes fail in HyDRa generated code", PmlPackage.Literals.IDENTIFIER__ATTRIBUTES, INVALID_NAME);
			}
		}
	}
}
