package be.unamur.polystore.jidbm;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.xtext.resource.XtextResource;
import org.eclipse.xtext.resource.XtextResourceSet;

import com.dbmain.jidbm.DBMAttribute;
import com.dbmain.jidbm.DBMAttributeOwner;
import com.dbmain.jidbm.DBMCollection;
import com.dbmain.jidbm.DBMCompoundAttribute;
import com.dbmain.jidbm.DBMConstraint;
import com.dbmain.jidbm.DBMConstraintMember;
import com.dbmain.jidbm.DBMEntityType;
import com.dbmain.jidbm.DBMGenericObject;
import com.dbmain.jidbm.DBMGroup;
import com.dbmain.jidbm.DBMLibrary;
import com.dbmain.jidbm.DBMProject;
import com.dbmain.jidbm.DBMRelationshipType;
import com.dbmain.jidbm.DBMRole;
import com.dbmain.jidbm.DBMSchema;
import com.dbmain.jidbm.DBMSimpleAttribute;
import com.dbmain.jidbmx.Groups;
import com.google.inject.Injector;

import be.unamur.polystore.PmlStandaloneSetup;
import be.unamur.polystore.pml.AbstractMappingRule;
import be.unamur.polystore.pml.AbstractPhysicalSchema;
import be.unamur.polystore.pml.ArrayField;
import be.unamur.polystore.pml.Attribute;
import be.unamur.polystore.pml.BracketsField;
import be.unamur.polystore.pml.Cardinality;
import be.unamur.polystore.pml.Collection;
import be.unamur.polystore.pml.DataType;
import be.unamur.polystore.pml.Database;
import be.unamur.polystore.pml.DocumentSchema;
import be.unamur.polystore.pml.Domainmodel;
import be.unamur.polystore.pml.EmbeddedObject;
import be.unamur.polystore.pml.EntityMappingRule;
import be.unamur.polystore.pml.EntityType;
import be.unamur.polystore.pml.Identifier;
import be.unamur.polystore.pml.IntType;
import be.unamur.polystore.pml.KVComplexField;
import be.unamur.polystore.pml.KVDataType;
import be.unamur.polystore.pml.Key;
import be.unamur.polystore.pml.KeyValuePair;
import be.unamur.polystore.pml.KeyValueSchema;
import be.unamur.polystore.pml.LongField;
import be.unamur.polystore.pml.PhysicalField;
import be.unamur.polystore.pml.Reference;
import be.unamur.polystore.pml.RelationalSchema;
import be.unamur.polystore.pml.RelationshipMappingRule;
import be.unamur.polystore.pml.RelationshipType;
import be.unamur.polystore.pml.Role;
import be.unamur.polystore.pml.RoleToEmbbededObjectMappingRule;
import be.unamur.polystore.pml.RoleToKeyBracketsFieldMappingRule;
import be.unamur.polystore.pml.RoleToReferenceMappingRule;
import be.unamur.polystore.pml.ShortField;
import be.unamur.polystore.pml.Table;
import be.unamur.polystore.pml.TerminalExpression;
import be.unamur.polystore.pml.impl.DataTypeImpl;

public class LunGenerator {
	private static final String TEMPLATE_LUN_PATH = "resources/schema.lun";
	private static final File TEMPLATE_LUN = new File(TEMPLATE_LUN_PATH);

	public String pmlFile;
	public String outputLun;
	DBMLibrary lib;
	private DBMProject project;
	private DBMSchema conceptualSchema;
	private DBMSchema physicalSchema;
	private DBMSchema dbSchema;
	private Domainmodel model;

	private Map<PhysicalField, DBMAttribute> attributes = new HashMap<PhysicalField, DBMAttribute>();
	private Map<Table, DBMEntityType> entitiesTables = new HashMap<Table, DBMEntityType>();
	private Map<Collection, DBMEntityType> entitiesCollections = new HashMap<Collection, DBMEntityType>();
	private Map<KeyValuePair, DBMEntityType> entitiesKVPairs = new HashMap<KeyValuePair, DBMEntityType>();
	private Map<Reference, DBMGroup> references = new HashMap<Reference, DBMGroup>();
	Map<AbstractPhysicalSchema, DBMCollection> schemas = new HashMap<AbstractPhysicalSchema, DBMCollection>();

	private List<DBMGroup> targetGroups = new ArrayList<DBMGroup>();

	public static void main(String[] args) throws IOException {
		String pmlFile = "C:/Users/lmeurice/Documents/HyDRa/be.unamur.polystore/resources/S1.pml";
		String outputLun = "C:/Users/lmeurice/Desktop/output.lun";

		LunGenerator generator = new LunGenerator(pmlFile, outputLun);
		generator.parse();
		generator.copyLunFile();
		generator.readLunFile();
		generator.extractConceptualSchema();
		generator.extractPhysicalSchema();
		generator.extractMappingRules();
		generator.extractDatabases();
		generator.extractMappingDatabases();
		generator.unload();
	}

	private void unload() {
		lib.unloadLUN(project.getProjectIdentifier(), outputLun);
	}

	public LunGenerator(String pml, String outputLun) {
		this.pmlFile = pml;
		this.outputLun = outputLun;
	}

	private void parse() {
		new org.eclipse.emf.mwe.utils.StandaloneSetup().setPlatformUri("../");
		Injector injector = new PmlStandaloneSetup().createInjectorAndDoEMFRegistration();
		XtextResourceSet resourceSet = injector.getInstance(XtextResourceSet.class);
		resourceSet.addLoadOption(XtextResource.OPTION_RESOLVE_ALL, Boolean.TRUE);
		URI modelURI = URI.createFileURI(pmlFile);
		Resource resource = resourceSet.getResource(modelURI, true);

		this.model = (Domainmodel) resource.getContents().get(0);
	}

	private void copyLunFile() throws IOException {
		Path copied = Paths.get(outputLun);
		Path originalPath = Paths.get(TEMPLATE_LUN.getAbsolutePath());
		Files.copy(originalPath, copied, StandardCopyOption.REPLACE_EXISTING);
	}

	private void readLunFile() throws IOException {
		this.lib = new DBMLibrary();
		this.project = lib.loadDBMProject(outputLun);
		this.conceptualSchema = getDBMSchema(project, "CONCEPTUAL SCHEMA");
		this.physicalSchema = getDBMSchema(project, "PHYSICAL SCHEMA");
		this.dbSchema = getDBMSchema(project, "DATABASES");
//		DBMSchema schema = pro.getFirstProductSchema();
//		schema.createEntityType("test", "test");
//		lib.unloadLUN(pro.getProjectIdentifier(), outputLun);
	}

	private DBMSchema getDBMSchema(DBMProject pro, String name) {
		if (pro == null)
			return null;

		DBMSchema schema = pro.getFirstProductSchema();
		while (schema != null) {
			if (schema.getName().equals(name))
				return schema;
			schema = pro.getNextProductSchema(schema);
		}

		return null;
	}


	private DBMGroup findGroup(List<DBMAttribute> attributes) {
		if (attributes == null || attributes.size() == 0)
			return null;

		for (DBMGroup gr : targetGroups)
			if (isEqual(attributes, gr))
				return gr;
		return null;
	}

	private boolean isEqual(List<DBMAttribute> attributes, DBMGroup gr) {
		Set<DBMAttribute> set = new HashSet<DBMAttribute>(attributes);
		int size = 0;
		DBMSimpleAttribute attr = gr.getFirstComponentSimpleAttribute();
		while (attr != null) {
			if (!set.contains(attr))
				return false;
			attr = gr.getNextComponentSimpleAttribute(attr);
			size++;
		}

		return set.size() == size;
	}

	private String getConceptualType(DataType type) {
		switch (type.getClass().getSimpleName()) {
		case "IntTypeImpl":
			return "int";
		case "BigintTypeImpl":
			return "bigint";
		case "StringTypeImpl":
			return "string";
		case "TextTypeImpl":
			return "text";
		case "BoolTypeImpl":
			return "boolean";
		case "FloatTypeImpl":
			return "float";
		case "BlobTypeImpl":
			return "blob";
		case "DateTypeImpl":
			return "date";
		case "DatetimeTypeImpl":
			return "datetime";
		default:
			return "";
		}

	}

	private void extractMappingRules() {
		for (AbstractMappingRule rule : model.getMappingRules().getMappingRules()) {
			if (rule instanceof EntityMappingRule)
				extractEntityMappingRule((EntityMappingRule) rule);
			if(rule instanceof RelationshipMappingRule)
				extractRelationshipMappingRule((RelationshipMappingRule) rule);
			if(rule instanceof RoleToEmbbededObjectMappingRule)
				extractRoleToEmbbededObjectMappingRule((RoleToEmbbededObjectMappingRule) rule);
			if(rule instanceof RoleToReferenceMappingRule)
				extractRoleToReferenceMappingRule((RoleToReferenceMappingRule) rule);
			if(rule instanceof RoleToKeyBracketsFieldMappingRule)
				extractRoleToKeyBracketsFieldMappingRule((RoleToKeyBracketsFieldMappingRule) rule);

		}
	}

	/**
	 * This method could not be tested. None of the devs can remember what a "RoleToEmbbededObjectMappingRule" is
	 * @param rule
	 */
	private void extractRoleToKeyBracketsFieldMappingRule(RoleToKeyBracketsFieldMappingRule rule) {
		Role role = rule.getRoleConceptual();
		RelationshipType relType = (RelationshipType) role.eContainer();
		
		DBMRelationshipType rel = this.conceptualSchema.findRelationshipType(relType.getName());
		DBMRole r = findRole(rel, role);
		DBMAttribute physicalAttr = attributes.get(rule.getKeyField());
		map(r, physicalAttr);
		
	}
	
	private void extractRoleToReferenceMappingRule(RoleToReferenceMappingRule rule) {
		Role role = rule.getRoleConceptual();
		RelationshipType relType = (RelationshipType) role.eContainer();
		
		DBMRelationshipType rel = this.conceptualSchema.findRelationshipType(relType.getName());
		DBMRole r = findRole(rel, role);
		DBMGroup group = references.get(rule.getReference());
		
		map(r, group);
	}
	
	private void extractRoleToEmbbededObjectMappingRule(RoleToEmbbededObjectMappingRule rule) {
		Role role = rule.getRoleConceptual();
		RelationshipType relType = (RelationshipType) role.eContainer();
		
		DBMRelationshipType rel = this.conceptualSchema.findRelationshipType(relType.getName());
		DBMRole r = findRole(rel, role);
		DBMAttribute physicalAttr = attributes.get(rule.getPhysicalStructure());
		map(r, physicalAttr);
	}
	
	private DBMRole findRole(DBMRelationshipType rel, Role role) {
		DBMRole r = rel.getFirstRole();
		while(r != null) {
			if(r.getName().equals(role.getName()))
				return r;
			r = rel.getNextRole(r);
		}
		
		return null;
	}

	private void extractEntityMappingRule(EntityMappingRule rule) {
		EntityType e = rule.getEntityConceptual();
		DBMEntityType ent = this.conceptualSchema.findEntityType(e.getName());
		for (int i = 0; i < rule.getAttributesConceptual().size(); i++) {
			DBMAttribute conceptualAttr = ent.findAttribute(rule.getAttributesConceptual().get(i).getName());
			DBMAttribute physicalAttr = attributes.get(rule.getPhysicalFields().get(i));

			map(conceptualAttr, physicalAttr);
		}
	}
	
	private void extractRelationshipMappingRule(RelationshipMappingRule rule) {
		RelationshipType rel = rule.getRelationshipConceptual();
		DBMRelationshipType relType = this.conceptualSchema.findRelationshipType(rel.getName());
		for (int i = 0; i < rule.getAttributesConceptual().size(); i++) {
			DBMAttribute conceptualAttr = relType.findAttribute(rule.getAttributesConceptual().get(i).getName());
			DBMAttribute physicalAttr = attributes.get(rule.getPhysicalFields().get(i));

			map(conceptualAttr, physicalAttr);
		}
	}

	private void extractPhysicalSchema() {
		for (AbstractPhysicalSchema schema : model.getPhysicalSchema().getPhysicalSchemas()) {
			DBMCollection collection = physicalSchema.createCollection(schema.getName(), schema.getName());
			schemas.put(schema, collection);
			if (schema instanceof RelationalSchema)
				createRelationalSchema(collection, (RelationalSchema) schema);
			if (schema instanceof DocumentSchema)
				createDocumentSchema(collection, (DocumentSchema) schema);
			if (schema instanceof KeyValueSchema)
				createKeyValueSchema(collection, (KeyValueSchema) schema);
		}

		for (AbstractPhysicalSchema schema : model.getPhysicalSchema().getPhysicalSchemas()) {
			if (schema instanceof RelationalSchema)
				createRelationalReferences((RelationalSchema) schema);
			if (schema instanceof DocumentSchema)
				createDocumentReferences((DocumentSchema) schema);
			if (schema instanceof KeyValueSchema)
				createKeyValueReferences((KeyValueSchema) schema);
		}
	}

	private void createDocumentReferences(DocumentSchema schema) {
		for (Collection coll : schema.getCollections()) {
			DBMEntityType e = entitiesCollections.get(coll);
			for (Reference ref : coll.getReferences()) {
				DBMGroup group = e.createGroup(ref.getName(), DBMGroup.ASS_GROUP, DBMGroup.NONE_GR, 0, 1, null);
				DBMGenericObject prev_comp = null;
				for (PhysicalField src : ref.getSourceField()) {
					DBMAttribute attr = attributes.get(src);
					group.addNextComponent(attr, prev_comp);
					prev_comp = attr;
				}

				List<DBMAttribute> targetGroup = new ArrayList<DBMAttribute>();
				for (PhysicalField target : ref.getTargetField())
					targetGroup.add(attributes.get(target));

				DBMGroup ref_group = findGroup(targetGroup);
				if (ref_group == null) {
					DBMEntityType target_ent = getDBMEntityType(targetGroup.get(0));
					int nbOfgrps = getNbOfGroups(target_ent);
					ref_group = target_ent.createGroup("gr_" + nbOfgrps, DBMGroup.ASS_GROUP, DBMGroup.NONE_GR, 0, 1,
							null);
					DBMAttribute prev_attr = null;
					for (DBMAttribute a : targetGroup) {
						ref_group.addNextComponent(a, prev_attr);
						prev_attr = a;
					}

				}

				Groups.createConstraint(group, ref_group, DBMConstraint.GEN_CONSTRAINT);
				references.put(ref, group);
			}
		}

	}

	private void createKeyValueReferences(KeyValueSchema schema) {
		for (KeyValuePair pair : schema.getKvpairs()) {
			DBMEntityType e = entitiesKVPairs.get(pair);
			for (Reference ref : pair.getReferences()) {
				DBMGroup group = e.createGroup(ref.getName(), DBMGroup.ASS_GROUP, DBMGroup.NONE_GR, 0, 1, null);
				DBMGenericObject prev_comp = null;
				for (PhysicalField src : ref.getSourceField()) {
					DBMAttribute attr = attributes.get(src);
					group.addNextComponent(attr, prev_comp);
					prev_comp = attr;
				}

				List<DBMAttribute> targetGroup = new ArrayList<DBMAttribute>();
				for (PhysicalField target : ref.getTargetField())
					targetGroup.add(attributes.get(target));

				DBMGroup ref_group = findGroup(targetGroup);
				if (ref_group == null) {
					DBMEntityType target_ent = getDBMEntityType(targetGroup.get(0));
					int nbOfgrps = getNbOfGroups(target_ent);
					ref_group = target_ent.createGroup("gr_" + nbOfgrps, DBMGroup.ASS_GROUP, DBMGroup.NONE_GR, 0, 1,
							null);
					DBMAttribute prev_attr = null;
					for (DBMAttribute a : targetGroup) {
						ref_group.addNextComponent(a, prev_attr);
						prev_attr = a;
					}

				}

				Groups.createConstraint(group, ref_group, DBMConstraint.GEN_CONSTRAINT);
				references.put(ref, group);
			}
		}
	}

	private void createRelationalReferences(RelationalSchema schema) {
		for (Table table : schema.getTables()) {
			DBMEntityType e = entitiesTables.get(table);
			for (Reference ref : table.getReferences()) {
				DBMGroup group = e.createGroup(ref.getName(), DBMGroup.ASS_GROUP, DBMGroup.NONE_GR, 0, 1, null);
				DBMGenericObject prev_comp = null;
				for (PhysicalField src : ref.getSourceField()) {
					DBMAttribute attr = attributes.get(src);
					group.addNextComponent(attr, prev_comp);
					prev_comp = attr;
				}

				List<DBMAttribute> targetGroup = new ArrayList<DBMAttribute>();
				for (PhysicalField target : ref.getTargetField())
					targetGroup.add(attributes.get(target));

				DBMGroup ref_group = findGroup(targetGroup);
				if (ref_group == null) {
					DBMEntityType target_ent = getDBMEntityType(targetGroup.get(0));
					int nbOfgrps = getNbOfGroups(target_ent);
					ref_group = target_ent.createGroup("gr_" + nbOfgrps, DBMGroup.ASS_GROUP, DBMGroup.NONE_GR, 0, 1,
							null);
					DBMAttribute prev_attr = null;
					for (DBMAttribute a : targetGroup) {
						ref_group.addNextComponent(a, prev_attr);
						prev_attr = a;
					}

				}

				Groups.createConstraint(group, ref_group, DBMConstraint.GEN_CONSTRAINT);
				references.put(ref, group);
			}
		}

	}

	private void createKeyValueSchema(DBMCollection collection, KeyValueSchema schema) {
		collection.setMetaPropertyValue("HyDRa type", "key-value");
		DBMEntityType prev = null;
		for (KeyValuePair kvpair : schema.getKvpairs()) {
			DBMEntityType t = physicalSchema.createEntityType(kvpair.getName(), kvpair.getName());
			entitiesKVPairs.put(kvpair, t);
			collection.addNextDataObject(t, prev);

			LinkedHashSet<String> variables = new LinkedHashSet<String>();
			DBMAttribute prev_attr = null;
			DBMAttribute prev_var = null;
			Key key = kvpair.getKey();
			String colName = "";
			for (TerminalExpression expr : key.getPattern()) {
				if (expr instanceof BracketsField) {
					String varName = ((BracketsField) expr).getName();
					int position = getVariablePosition(variables, varName);
					if (position == -1) {
						variables.add(varName);
						position = variables.size();
						prev_var = t.createSimpleAttribute(varName, varName, 1, 1, ' ', DBMSimpleAttribute.VARCHAR_ATT,
								true, false, 15, (short) 0, this.physicalSchema, prev_var);
						addStringtoListMetaproperty(prev_var, "Stereotype", "var");
						attributes.put((BracketsField) expr, prev_var);
					}
					colName += "[" + position + "]";
				} else
					colName += "\"" + expr.getLiteral() + "\"";

			}

			prev_attr = t.createSimpleAttribute(colName, colName, 1, 1, ' ', DBMSimpleAttribute.VARCHAR_ATT, true,
					false, 15, (short) 0, this.physicalSchema, prev_attr);
			addStringtoListMetaproperty(prev_attr, "Stereotype", "key");

			PhysicalField value = kvpair.getValue();
			if (value instanceof ShortField) {
				ShortField sf = (ShortField) value;
				prev_attr = t.createSimpleAttribute(sf.getName(), sf.getName(), 1, 1, ' ',
						DBMSimpleAttribute.VARCHAR_ATT, true, false, 15, (short) 0, this.physicalSchema, prev_attr);
				addStringtoListMetaproperty(prev_attr, "Stereotype", "value");
				attributes.put(sf, prev_attr);
			}

			if (value instanceof LongField) {
				LongField lf = (LongField) value;
				colName = "";
				for (TerminalExpression expr : lf.getPattern()) {
					if (expr instanceof BracketsField) {
						String varName = ((BracketsField) expr).getName();
						int position = getVariablePosition(variables, varName);
						if (position == -1) {
							variables.add(varName);
							position = variables.size();
							prev_var = t.createSimpleAttribute(varName, varName, 1, 1, ' ',
									DBMSimpleAttribute.VARCHAR_ATT, true, false, 15, (short) 0, this.physicalSchema,
									prev_var);
							addStringtoListMetaproperty(prev_var, "Stereotype", "var");
							attributes.put((BracketsField) expr, prev_var);
						}
						colName += "[" + position + "]";
					} else
						colName += "\"" + expr.getLiteral() + "\"";

				}

				prev_attr = t.createSimpleAttribute(colName, colName, 1, 1, ' ', DBMSimpleAttribute.VARCHAR_ATT, true,
						false, 15, (short) 0, this.physicalSchema, prev_attr);
				addStringtoListMetaproperty(prev_attr, "Stereotype", "value");
				addStringtoListMetaproperty(prev_attr, "Stereotype", "comp");
			}

			if (value instanceof KVComplexField) {
				KVComplexField cf = (KVComplexField) value;
				String valueType = cf.getType().getLiteral();
				switch (valueType) {
				case "hash":
					prev_attr = t.createCompoundAttribute("hash", "hash", 1, 1, ' ', this.physicalSchema, prev_attr);
					addStringtoListMetaproperty(prev_attr, "Stereotype", "value");
					DBMAttribute prev_sub = null;
					for (PhysicalField pf : cf.getFields()) {
						if (pf instanceof ShortField) {
							ShortField sf = (ShortField) pf;
							prev_sub = ((DBMCompoundAttribute) prev_attr).createSimpleAttribute(sf.getName(),
									sf.getName(), 1, 1, ' ', DBMSimpleAttribute.VARCHAR_ATT, true, false, 15, (short) 0,
									this.physicalSchema, prev_sub);
							attributes.put(sf, prev_sub);
						}

						if (pf instanceof LongField) {
							LongField lf = (LongField) pf;
							colName = "";
							for (TerminalExpression expr : lf.getPattern()) {
								if (expr instanceof BracketsField) {
									String varName = ((BracketsField) expr).getName();
									int position = getVariablePosition(variables, varName);
									if (position == -1) {
										variables.add(varName);
										position = variables.size();
										prev_var = t.createSimpleAttribute(varName, varName, 1, 1, ' ',
												DBMSimpleAttribute.VARCHAR_ATT, true, false, 15, (short) 0,
												this.physicalSchema, prev_var);
										addStringtoListMetaproperty(prev_var, "Stereotype", "var");
										attributes.put((BracketsField) expr, prev_var);
									}
									colName += "[" + position + "]";
								} else
									colName += "\"" + expr.getLiteral() + "\"";

							}

							prev_attr = t.createSimpleAttribute(colName, colName, 1, 1, ' ',
									DBMSimpleAttribute.VARCHAR_ATT, true, false, 15, (short) 0, this.physicalSchema,
									prev_attr);
							addStringtoListMetaproperty(prev_attr, "Stereotype", "value");
							addStringtoListMetaproperty(prev_attr, "Stereotype", "comp");
						}
					}
					break;
				case "list":
				case "set":
				case "ordered set":
					ShortField sf = (ShortField) cf.getFields().get(0);

					prev_attr = t.createSimpleAttribute(sf.getName(), sf.getName(), 0, DBMRole.N_CARD, ' ',
							DBMSimpleAttribute.VARCHAR_ATT, true, false, 15, (short) 0, this.physicalSchema, prev_attr);
					prev_attr.setMetaPropertyValue("HyDRa type", valueType);
					addStringtoListMetaproperty(prev_attr, "Stereotype", "value");
					attributes.put(sf, prev_attr);
					break;
				}
			}

			prev = t;
		}
	}

	private void createDocumentSchema(DBMCollection collection, DocumentSchema schema) {
		collection.setMetaPropertyValue("HyDRa type", "document");
		DBMEntityType prev = null;
		for (Collection coll : schema.getCollections()) {
			DBMEntityType t = physicalSchema.createEntityType(coll.getName(), coll.getName());
			collection.addNextDataObject(t, prev);
			entitiesCollections.put(coll, t);

			LinkedHashSet<String> variables = new LinkedHashSet<String>();
			DBMAttribute prev_attr = null;
			DBMAttribute prev_var = null;
			for (PhysicalField pf : coll.getFields()) {
				prev_attr = createDocumentField(t, t, pf, prev_attr, prev_var, variables);
			}

			prev = t;
		}

	}

	private DBMAttribute createDocumentField(final DBMEntityType collection, DBMAttributeOwner parent, PhysicalField pf,
			DBMAttribute prev_attr, DBMAttribute prev_var, LinkedHashSet<String> variables) {

		if (pf instanceof ShortField) {
			ShortField sf = (ShortField) pf;
			prev_attr = parent.createSimpleAttribute(sf.getName(), sf.getName(), 1, 1, ' ',
					DBMSimpleAttribute.VARCHAR_ATT, true, false, 15, (short) 0, this.physicalSchema, prev_attr);
			attributes.put(sf, prev_attr);
		}

		if (pf instanceof LongField) {
			LongField lf = (LongField) pf;
			String colName = ((LongField) pf).getPhysicalName() + ": ";
			for (TerminalExpression expr : lf.getPattern()) {
				if (expr instanceof BracketsField) {
					String varName = ((BracketsField) expr).getName();
					int position = getVariablePosition(variables, varName);
					if (position == -1) {
						variables.add(varName);
						position = variables.size();
						prev_var = collection.createSimpleAttribute(varName, varName, 1, 1, ' ',
								DBMSimpleAttribute.VARCHAR_ATT, true, false, 15, (short) 0, this.physicalSchema,
								prev_var);

						addStringtoListMetaproperty(prev_var, "Stereotype", "var");
						attributes.put((BracketsField) expr, prev_var);
					}
					colName += "[" + position + "]";
				} else
					colName += "\"" + expr.getLiteral() + "\"";

			}

			prev_attr = parent.createSimpleAttribute(colName, colName, 1, 1, ' ', DBMSimpleAttribute.VARCHAR_ATT, true,
					false, 15, (short) 0, this.physicalSchema, prev_attr);
			addStringtoListMetaproperty(prev_attr, "Stereotype", "comp");
		}

		if (pf instanceof EmbeddedObject) {
			EmbeddedObject eo = (EmbeddedObject) pf;
			prev_attr = parent.createCompoundAttribute(eo.getName(), eo.getName(),
					eo.getCardinality().getValue() == Cardinality.ZERO_MANY_VALUE
							|| eo.getCardinality().getValue() == Cardinality.ZERO_ONE_VALUE ? 0 : 1,
					eo.getCardinality().getValue() == Cardinality.ONE_VALUE
							|| eo.getCardinality().getValue() == Cardinality.ZERO_ONE_VALUE ? 1 : DBMRole.N_CARD,
					' ', this.physicalSchema, prev_attr);
			attributes.put(eo, prev_attr);

			DBMAttribute prev_sub = null;
			for (PhysicalField pf2 : eo.getFields())
				prev_sub = createDocumentField(collection, (DBMCompoundAttribute) prev_attr, pf2, prev_sub, prev_var,
						variables);
		}

		if (pf instanceof ArrayField) {
			ArrayField af = (ArrayField) pf;
			String varName = af.getName();
			int position = getVariablePosition(variables, varName);
			if (position == -1) {
				variables.add(varName);
				position = variables.size();
				prev_var = collection.createSimpleAttribute(varName, varName, 1, 1, ' ', DBMSimpleAttribute.VARCHAR_ATT,
						true, false, 15, (short) 0, this.physicalSchema, prev_var);
				attributes.put(af, prev_var);
				addStringtoListMetaproperty(prev_var, "Stereotype", "var");
			}

			String colName = af.getPhysicalName() + ": [" + position + "]";
			prev_attr = parent.createSimpleAttribute(colName, colName, 0, DBMRole.N_CARD, ' ',
					DBMSimpleAttribute.VARCHAR_ATT, true, false, 15, (short) 0, this.physicalSchema, prev_attr);
			addStringtoListMetaproperty(prev_attr, "Stereotype", "comp");
			addStringtoListMetaproperty(prev_attr, "HyDRa type", "list");

		}

		return prev_attr;
	}

	private void createRelationalSchema(DBMCollection collection, RelationalSchema schema) {

		collection.setMetaPropertyValue("HyDRa type", "relational");
		DBMEntityType prev = null;
		for (Table table : schema.getTables()) {
			DBMEntityType t = physicalSchema.createEntityType(table.getName(), table.getName());
			entitiesTables.put(table, t);
			collection.addNextDataObject(t, prev);

			LinkedHashSet<String> variables = new LinkedHashSet<String>();
			DBMAttribute prev_attr = null;
			DBMAttribute prev_var = null;
			for (PhysicalField pf : table.getColumns()) {
				if (pf instanceof ShortField) {
					ShortField sf = (ShortField) pf;
					prev_attr = t.createSimpleAttribute(sf.getName(), sf.getName(), 1, 1, ' ',
							DBMSimpleAttribute.VARCHAR_ATT, true, false, 15, (short) 0, this.physicalSchema, prev_attr);
					attributes.put(sf, prev_attr);

				}

				if (pf instanceof LongField) {
					LongField lf = (LongField) pf;
					String colName = ((LongField) pf).getPhysicalName() + ": ";
					for (TerminalExpression expr : lf.getPattern()) {
						if (expr instanceof BracketsField) {
							String varName = ((BracketsField) expr).getName();
							int position = getVariablePosition(variables, varName);
							if (position == -1) {
								variables.add(varName);
								position = variables.size();
								prev_var = t.createSimpleAttribute(varName, varName, 1, 1, ' ',
										DBMSimpleAttribute.VARCHAR_ATT, true, false, 15, (short) 0, this.physicalSchema,
										prev_var);

								attributes.put((BracketsField) expr, prev_var);

								addStringtoListMetaproperty(prev_var, "Stereotype", "var");
							}
							colName += "[" + position + "]";
						} else
							colName += "\"" + expr.getLiteral() + "\"";

					}

					prev_attr = t.createSimpleAttribute(colName, colName, 1, 1, ' ', DBMSimpleAttribute.VARCHAR_ATT,
							true, false, 15, (short) 0, this.physicalSchema, prev_attr);
					addStringtoListMetaproperty(prev_attr, "Stereotype", "comp");
				}
			}

			prev = t;
		}

	}

	private int getNbOfGroups(DBMEntityType ent) {
		int size = 0;
		DBMGroup g = ent.getFirstGroup();
		while (g != null) {
			size++;
			g = ent.getNextGroup(g);
		}

		return size;
	}

	private DBMEntityType getDBMEntityType(DBMAttribute a) {
		DBMGenericObject o = a.getAttributeOwner();
		if (o instanceof DBMEntityType)
			return (DBMEntityType) o;
		if (o instanceof DBMAttribute)
			return getDBMEntityType((DBMAttribute) o);
		return null;
	}

	private void addStringtoListMetaproperty(DBMAttribute attr, String mp, String value) {
		if (attr == null || mp == null || value == null)
			return;
		Vector<String> list = attr.getMetaPropertyStringListValue(mp);
		if (list == null)
			list = new Vector<String>();
		list.add(value);
		attr.setMetaPropertyStringListValue(mp, list);
	}
	
	private void addStringtoStringMetaproperty(DBMGenericObject attr, String mp, String value) {
		if (attr == null || mp == null || value == null)
			return;
		attr.setMetaPropertyStringValue(mp, value);
	}
	
	private void addInttoIntMetaproperty(DBMGenericObject attr, String mp, Integer value) {
		if (attr == null || mp == null || value == null)
			return;
		attr.setMetaPropertyIntValue(mp, value);
	}

	private int getVariablePosition(LinkedHashSet<String> variables, String varName) {
		int index = 1;
		for (String var : variables) {
			if (var.equals(varName))
				return index;
			index++;
		}

		return -1;
	}

	private void extractConceptualSchema() {
		Map<String, DBMEntityType> entities = new HashMap<String, DBMEntityType>();
		for (EntityType ent : model.getConceptualSchema().getEntities()) {
			DBMEntityType entity = conceptualSchema.createEntityType(ent.getName(), ent.getName());
			entities.put(ent.getName(), entity);
			DBMAttribute prev = null;
			Map<String, DBMAttribute> attributes = new HashMap<String, DBMAttribute>();
			for (Attribute attr : ent.getAttributes()) {
				prev = entity.createSimpleAttribute(attr.getName(), attr.getName(), 1, 1, ' ',
						DBMSimpleAttribute.VARCHAR_ATT, true, false, 15, (short) 0, this.conceptualSchema, prev);
				prev.setMetaPropertyValue("HyDRa type", getConceptualType(attr.getType()));
				attributes.put(attr.getName(), prev);
			}

			Identifier id = ent.getIdentifier();
			if (id != null) {
				DBMGroup group = entity.createGroup("ID" + entity.getName(), DBMGroup.ASS_GROUP, DBMGroup.PRIM_GR, 0, 1,
						null);
				prev = null;
				for (Attribute attr : id.getAttributes()) {
					group.addNextComponent(attributes.get(attr.getName()), prev);
				}
			}
		}

		for (RelationshipType rel : model.getConceptualSchema().getRelationships()) {
			DBMRelationshipType rtype = conceptualSchema.createRelationshipType(rel.getName(), rel.getName());
			DBMAttribute prev = null;
			for (Attribute attr : rel.getAttributes()) {
				prev = rtype.createSimpleAttribute(attr.getName(), attr.getName(), 1, 1, ' ',
						DBMSimpleAttribute.VARCHAR_ATT, true, false, 15, (short) 0, this.conceptualSchema, prev);
				prev.setMetaPropertyStringValue("HyDRa type", getConceptualType(attr.getType()));
			}

			for (Role role : rel.getRoles()) {
				DBMRole r = rtype.createRole(role.getName(),
						role.getCardinality().getValue() == Cardinality.ZERO_MANY_VALUE
								|| role.getCardinality().getValue() == Cardinality.ZERO_ONE_VALUE ? 0 : 1,
						role.getCardinality().getValue() == Cardinality.ONE_VALUE
								|| role.getCardinality().getValue() == Cardinality.ZERO_ONE_VALUE ? 1 : DBMRole.N_CARD,
						' ');
				r.addFirstEntityType(entities.get(role.getEntity().getName()));
			}
		}

	}
	
	private void extractDatabases() {
		for(Database db : model.getDatabases().getDatabases()) {
			String name = db.getName();
			String host = db.getHost();
			String dbName = db.getDatabaseName();
			int port = db.getPort();
			String login = db.getLogin();
			String password = db.getPassword();
			String dbType = db.getDbType().getLiteral();
			DBMCollection coll = dbSchema.createCollection(name, name);
			addStringtoStringMetaproperty(coll, "db type", dbType);
			addStringtoStringMetaproperty(coll, "host", host);
			addStringtoStringMetaproperty(coll, "db name", dbName);
			addStringtoStringMetaproperty(coll, "login", login);
			addStringtoStringMetaproperty(coll, "password", password);
			addInttoIntMetaproperty(coll, "port", port);
			
		}
	}
	
	private void extractMappingDatabases() {
		for(AbstractPhysicalSchema schema : model.getPhysicalSchema().getPhysicalSchemas()) {
			DBMCollection schemaColl = schemas.get(schema);
			for(Database db : schema.getDatabases()) {
				DBMCollection dbColl = findCollection(dbSchema, db.getName());
				map(dbColl, schemaColl);
			}
		}
	}

	private DBMCollection findCollection(DBMSchema sch, String collName) {
		DBMCollection coll = sch.getFirstCollection();
		while(coll != null) {
			if(coll.getName().equals(collName))
				return coll;
			coll = sch.getNextCollection(coll);
		}
		return null;
	}

	private void map(DBMGenericObject conceptual, DBMGenericObject physical) {
		Vector<Integer> v1 = conceptual.getMetaPropertyIntListValue("MappingOID");
		Vector<Integer> v2 = physical.getMetaPropertyIntListValue("MappingOID");

		if (v1 == null)
			v1 = new Vector<Integer>();
		if (v2 == null)
			v2 = new Vector<Integer>();

		Vector<Integer> v3 = new Vector<Integer>();
		v3.addAll(v1);
		v3.addAll(v2);

		conceptual.setMetaPropertyIntListValue("MappingOID", v3);
		physical.setMetaPropertyIntListValue("MappingOID", v3);
	}

}
