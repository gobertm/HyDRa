/*
 * generated by Xtext 2.23.0
 */
package be.unamur.polystore.scoping;

import org.eclipse.emf.common.util.BasicEList;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.xtext.EcoreUtil2;
import org.eclipse.xtext.scoping.IScope;
import org.eclipse.xtext.scoping.Scopes;

import be.unamur.polystore.pml.AbstractMappingRule;
import be.unamur.polystore.pml.AbstractPhysicalStructure;
import be.unamur.polystore.pml.BracketsField;
import be.unamur.polystore.pml.Collection;
import be.unamur.polystore.pml.ColumnFamily;
import be.unamur.polystore.pml.Edge;
import be.unamur.polystore.pml.EmbeddedObject;
import be.unamur.polystore.pml.EntityMappingRule;
import be.unamur.polystore.pml.LongField;
import be.unamur.polystore.pml.Node;
import be.unamur.polystore.pml.PhysicalField;
import be.unamur.polystore.pml.PmlPackage;
import be.unamur.polystore.pml.RoleMappingRule;
import be.unamur.polystore.pml.ShortField;
import be.unamur.polystore.pml.Table;
import be.unamur.polystore.pml.TableColumnDB;
import be.unamur.polystore.pml.TerminalExpression;

/**
 * This class contains custom scoping description.
 * 
 * See https://www.eclipse.org/Xtext/documentation/303_runtime_concepts.html#scoping
 * on how and when to use it.
 */
public class PmlScopeProvider extends AbstractPmlScopeProvider {

	@Override
	public IScope getScope(EObject context, EReference reference) {
		if (context instanceof EntityMappingRule && reference == PmlPackage.Literals.ENTITY_MAPPING_RULE__ATTRIBUTES_CONCEPTUAL) {
			EntityMappingRule rule = EcoreUtil2.getContainerOfType(context, EntityMappingRule.class);
			return Scopes.scopeFor(rule.getEntityConceptual().getAttributes());
		}
		if(context instanceof AbstractMappingRule && reference == PmlPackage.Literals.ABSTRACT_MAPPING_RULE__PHYSICAL_FIELDS) {
			AbstractMappingRule rule = EcoreUtil2.getContainerOfType(context, AbstractMappingRule.class);
			AbstractPhysicalStructure struct= rule.getPhysicalStructure();
			EList<PhysicalField> fields = new BasicEList<PhysicalField>();
			if(struct instanceof Table) 
				fields=((Table) struct).getColumns();
			if(struct instanceof Collection) 
				fields=((Collection) struct).getFields();
			if(struct instanceof EmbeddedObject)
				fields=((EmbeddedObject) struct).getFields();
			if(struct instanceof Node)
				fields= ((Node) struct).getFields();
			if(struct instanceof Edge)
				fields= ((Edge)struct).getFields();
			if(struct instanceof TableColumnDB) {
				EList<ColumnFamily> columnFamilies = ((TableColumnDB) struct).getColumnfamilies();
				for(ColumnFamily cf : columnFamilies)
					fields.addAll(cf.getColumns());
			}
			EList<PhysicalField> fieldsInComplex;
			fieldsInComplex= getFieldsFromLongField(fields);
			IScope scope = Scopes.scopeFor(fields);
			//fields.addAll(fieldsInComplex);
			return Scopes.scopeFor(fieldsInComplex,scope);
		}
		return super.getScope(context, reference);
	}

	public EList<PhysicalField> getFieldsFromLongField(EList<PhysicalField> fields){
		EList<PhysicalField> fieldsInComplex = new BasicEList<PhysicalField>();
		for(PhysicalField field : fields) {
			if(field instanceof LongField){
				for(TerminalExpression terminal : ((LongField)field).getPattern()) {
					if(terminal instanceof BracketsField)
						fieldsInComplex.add((BracketsField)terminal);
				}
			}
		}
		return fieldsInComplex;
	}
}
