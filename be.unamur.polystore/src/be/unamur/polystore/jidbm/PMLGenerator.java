package be.unamur.polystore.jidbm;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.dbmain.jidbm.DBMAttribute;
import com.dbmain.jidbm.DBMAttributeOwner;
import com.dbmain.jidbm.DBMCollection;
import com.dbmain.jidbm.DBMCompoundAttribute;
import com.dbmain.jidbm.DBMConcreteObject;
import com.dbmain.jidbm.DBMDataObject;
import com.dbmain.jidbm.DBMEntityRelationshipType;
import com.dbmain.jidbm.DBMEntityType;
import com.dbmain.jidbm.DBMGenericObject;
import com.dbmain.jidbm.DBMGroup;
import com.dbmain.jidbm.DBMLibrary;
import com.dbmain.jidbm.DBMProject;
import com.dbmain.jidbm.DBMRelationshipType;
import com.dbmain.jidbm.DBMRole;
import com.dbmain.jidbm.DBMSchema;
import com.dbmain.jidbm.DBMSimpleAttribute;

import be.unamur.polystore.pml.EntityType;

public class PMLGenerator {
	private static final String TAB = "   ";

	private StringBuilder conceptualSchemaStr = new StringBuilder();
	private StringBuilder physicalSchemaStr = new StringBuilder();
	private StringBuilder databasesStr = new StringBuilder();
	private StringBuilder mappingsStr = new StringBuilder();

	private DBMLibrary lib;
	private DBMProject project;
	private DBMSchema conceptualSchema;
	private DBMSchema physicalSchema;
	private DBMSchema dbSchema;

	private String conceptualSchemaName = "cs";

	public PMLGenerator(String lun) throws IOException {
		this.lib = new DBMLibrary();
		this.project = lib.loadDBMProject(lun);
		this.conceptualSchema = getDBMSchema(project, Constants.CONCEPTUAL_SCHEMA);
		this.physicalSchema = getDBMSchema(project, Constants.PHYSICAL_SCHEMA);
		this.dbSchema = getDBMSchema(project, Constants.DATABASE_SCHEMA);
	}

	public static void main(String[] args) throws Exception {
		String lun = "C:/Users/lmeurice/Desktop/output2.lun";
		lun = "C:/Users/lmeurice/Documents/SECO-ASSIST/HyDRa/Use-Cases/LUN/schema.lun";
		String outputPML = "C:/Users/lmeurice/Desktop/output2.pml";

		PMLGenerator generator = new PMLGenerator(lun);

		generator.readConceptualSchema();
		generator.readPhysicalSchema();
		generator.readDatabases();
		generator.readMappings();

		generator.generatePML(outputPML);
	}

	private void generatePML(String pml) throws IOException {
		File file = new File(pml);
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
			writer.append(conceptualSchemaStr);
			writer.append("\n");
			writer.append(physicalSchemaStr);
			writer.append("\n");
			writer.append(mappingsStr);
			writer.append("\n");
			writer.append(databasesStr);
		}
	}

	private void readConceptualSchema() {
		String v = conceptualSchema.getShortName();
		if (v != null && !v.isEmpty())
			conceptualSchemaName = v;

		conceptualSchemaStr.append("conceptual schema " + conceptualSchemaName + " {\n\n");
		DBMEntityType ent = conceptualSchema.getFirstDataObjectEntityType();
		while (ent != null) {
			conceptualSchemaStr.append(TAB + "entity type " + ent.getName() + " {\n");
			DBMAttribute attr = ent.getFirstAttribute();
			int cnt = 0;
			while (attr != null) {
				if (cnt > 0)
					conceptualSchemaStr.append(",\n");

				conceptualSchemaStr.append(
						TAB + TAB + attr.getName() + " : " + attr.getMetaPropertyStringValue(Constants.HYDRA_TYPE_MP));
				attr = ent.getNextAttribute(attr);
				cnt++;
			}
			conceptualSchemaStr.append("\n");

			DBMGroup group = ent.getFirstGroup();
			while (group != null) {
				conceptualSchemaStr.append(TAB + TAB + "identifier {\n");
				DBMSimpleAttribute attr2 = group.getFirstComponentSimpleAttribute();
				int cnt2 = 0;
				while (attr2 != null) {
					if (cnt2 > 0)
						conceptualSchemaStr.append(",\n");
					conceptualSchemaStr.append(TAB + TAB + TAB + attr2.getName());
					attr2 = group.getNextComponentSimpleAttribute(attr2);
					cnt2++;
				}
				conceptualSchemaStr.append("\n");
				conceptualSchemaStr.append(TAB + TAB + "}\n");
				group = ent.getNextGroup(group);
			}

			conceptualSchemaStr.append(TAB + "}\n\n");
			ent = conceptualSchema.getNextDataObjectEntityType(ent);
		}

		conceptualSchemaStr.append("\n");

		DBMRelationshipType rel = conceptualSchema.getFirstDataObjectRelationshipType();
		while (rel != null) {
			conceptualSchemaStr.append(TAB + "relationship type " + rel.getName() + " {\n");
			DBMRole role = rel.getFirstRole();
			int cnt = 0;
			while (role != null) {
				if (cnt > 0)
					conceptualSchemaStr.append(",\n");
				String roleName = role.getName();
				if(role == null || role.getName().trim().isEmpty()) {
					roleName = role.getFirstEntityType().getName();
					role.setName(roleName);
				}
								
				conceptualSchemaStr.append(TAB + TAB
						+ roleName
						+ getRoleStr(role) + " : " + role.getFirstEntityType().getName());

				role = rel.getNextRole(role);
				cnt++;
			}

			DBMAttribute attr = rel.getFirstAttribute();
			cnt = 0;
			if (attr != null)
				conceptualSchemaStr.append(",\n");
			while (attr != null) {
				if (cnt > 0)
					conceptualSchemaStr.append(",\n");
				conceptualSchemaStr.append(
						TAB + TAB + attr.getName() + " : " + attr.getMetaPropertyStringValue(Constants.HYDRA_TYPE_MP));

				attr = rel.getNextAttribute(attr);
				cnt++;
			}

			conceptualSchemaStr.append("\n");
			conceptualSchemaStr.append(TAB + "}\n\n");
			rel = conceptualSchema.getNextDataObjectRelationshipType(rel);
		}

		conceptualSchemaStr.append("}\n");

	}

	private void readPhysicalSchema() throws Exception {
		physicalSchemaStr.append("physical schemas {\n\n");
		DBMCollection schema = physicalSchema.getFirstCollection();
		while (schema != null) {
			String type = schema.getMetaPropertyStringValue(Constants.HYDRA_TYPE_MP);
			String schemaType = "?";
			switch (type) {
			case Constants.DOCUMENTSCHEMA:
				schemaType = "document";
				break;
			case Constants.RELATIONSCHEMA:
				schemaType = "relational";
				break;
			case Constants.KVSCHEMA:
				schemaType = "key value";
				break;
			}

			physicalSchemaStr
					.append(TAB + schemaType + " schema " + schema.getName() + getDatabasesMappings(schema) + " {\n\n");

			switch (type) {
			case Constants.DOCUMENTSCHEMA:
				readDocumentSchema(schema);
				break;
			case Constants.RELATIONSCHEMA:
				readRelationSchema(schema);
				break;
			case Constants.KVSCHEMA:
				readKeyValueSchema(schema);
				break;
			}

			physicalSchemaStr.append(TAB + "}");

			physicalSchemaStr.append("\n\n");

			schema = physicalSchema.getNextCollection(schema);
		}
		physicalSchemaStr.append("}\n");
	}

	private boolean readDocumentAttribute(DBMAttribute a, Map<DBMSimpleAttribute, Integer> variables, int TAB_LVL)
			throws Exception {
		boolean added = false;
		if (a instanceof DBMSimpleAttribute) {
			DBMSimpleAttribute attr = (DBMSimpleAttribute) a;

			if (attr.getMetaPropertyStringListValue(Constants.STEREOTYPE) == null
					|| !attr.getMetaPropertyStringListValue(Constants.STEREOTYPE).contains(Constants.VARIABLE)) {
				if (attr.getMetaPropertyStringListValue(Constants.STEREOTYPE) == null || !attr
						.getMetaPropertyStringListValue(Constants.STEREOTYPE).contains(Constants.COMPOSED_FIELD)) {
					// normal column
					physicalSchemaStr.append(TAB(TAB_LVL) + attr.getName());
				} else {

					if (attr.getMaximumCardinality() == DBMRole.N_CARD) {
						// ArrayField e.g., orders[]: Orderref
						String arrayField = readArrayField(attr, variables);
						physicalSchemaStr.append(TAB(TAB_LVL) + arrayField);
					} else {
						// composed field
						String composedField = readComposedField(attr, variables);
						physicalSchemaStr.append(TAB(TAB_LVL) + composedField);
					}
				}
				added = true;
			}
		}

		if (a instanceof DBMCompoundAttribute) {
			String card = "";
			if (a.getMaximumCardinality() == 1)
				card = "[1]";
			else if (a.getMaximumCardinality() > 1)
				card = "[0-N]";
			else
				throw new Exception("Invalid cardinality for nested field: " + getAbsoluteEmbeddedAttributeName(a)
						+ "=>" + a.getMaximumCardinality());

			physicalSchemaStr.append(TAB(TAB_LVL) + a.getName() + card + "{\n");

			DBMAttribute a2 = ((DBMCompoundAttribute) a).getFirstAttribute();
			int cnt = 0;
			while (a2 != null) {
				if (cnt > 0)
					physicalSchemaStr.append(",\n");
				boolean ok = readDocumentAttribute(a2, variables, TAB_LVL + 1);
				a2 = ((DBMCompoundAttribute) a).getNextAttribute(a2);
				if (ok)
					cnt++;
			}

			physicalSchemaStr.append("\n" + TAB(TAB_LVL) + "}");
			added = true;
		}

		return added;
	}

	private void readKeyValueSchema(DBMCollection schema) throws Exception {
		DBMEntityType ent = schema.getFirstDataObjectEntityType();
		while (ent != null) {
			physicalSchemaStr.append(TAB + TAB + "kvpairs " + ent.getName() + " {\n");
			Map<DBMSimpleAttribute, Integer> variables = new HashMap<DBMSimpleAttribute, Integer>();
			DBMSimpleAttribute attr = ent.getFirstAttributeSimpleAttribute();
			while (attr != null) {
				if (attr.getMetaPropertyStringListValue(Constants.STEREOTYPE) != null
						&& attr.getMetaPropertyStringListValue(Constants.STEREOTYPE).contains(Constants.VARIABLE)) {
					variables.put(attr, variables.size() + 1);
				}
				attr = ent.getNextAttributeSimpleAttribute(attr);
			}

			DBMAttribute key = getRedisKey(ent);
			if (key == null)
				throw new Exception("There is no explicit key defined in kvpairs: " + ent.getName());

			DBMAttribute value = getRedisValue(ent);
			if (value == null)
				throw new Exception("There is no explicit value defined in kvpairs: " + ent.getName());

			String keyName = key.getName();
			for (Entry<DBMSimpleAttribute, Integer> entry : variables.entrySet()) {
				DBMSimpleAttribute a = entry.getKey();
				int nb = entry.getValue();
				keyName = keyName.replaceAll("\\[" + nb + "\\]", "[" + a.getName() + "]");
			}

			physicalSchemaStr.append(TAB(3) + "key: " + keyName + ",\n");
			String valueStr = readRedisValue(value, variables);

			physicalSchemaStr.append(TAB(3) + "value: " + valueStr);
			physicalSchemaStr.append("\n");

			Map<DBMGroup, DBMGroup> references = new LinkedHashMap<DBMGroup, DBMGroup>();
			DBMGroup g = ent.getFirstGroup();
			while (g != null) {
				if (g.getFirstConstraintOrigin() != null) {
					DBMGroup targetGrp = g.getFirstConstraintOrigin().getFirstConstraintMember().getGroup();
					references.put(g, targetGrp);
				}
				g = ent.getNextGroup(g);
			}

			if (references.size() > 0) {
				int randomNameCnt = 1;
				physicalSchemaStr.append("\n" + TAB(3) + "references {\n");
				for (Entry<DBMGroup, DBMGroup> entry : references.entrySet()) {
					DBMGroup origin = entry.getKey();
					DBMGroup target = entry.getValue();
					String refName = origin.getName();
					if (refName == null || refName.trim().isEmpty()) {
						// random ref name
						refName = "ref" + randomNameCnt;
						origin.setName(refName);
						randomNameCnt++;
					}

					String referenceStr = refName + ": ";
					DBMGenericObject comp = origin.getFirstComponent();
					int cnt2 = 0;
					while (comp != null) {
						if (cnt2 > 0)
							referenceStr += ", ";
						DBMAttribute a = (DBMAttribute) comp;
						referenceStr += a.getName();
						comp = origin.getNextComponent(comp);
						cnt2++;
					}

					referenceStr += " -> ";

					cnt2 = 0;
					comp = target.getFirstComponent();
					while (comp != null) {
						if (cnt2 > 0)
							referenceStr += ", ";
						DBMAttribute a = (DBMAttribute) comp;
						referenceStr += getAbsoluteEmbeddedAttributeName(a);
						comp = target.getNextComponent(comp);
						cnt2++;
					}

					physicalSchemaStr.append(TAB(4) + referenceStr + "\n");
				}
				physicalSchemaStr.append(TAB(3) + "}\n");
			}

			physicalSchemaStr.append(TAB + TAB + "}\n\n");
			ent = schema.getNextDataObjectEntityType(ent);
		}
	}

	private String readRedisValue(DBMAttribute value, Map<DBMSimpleAttribute, Integer> variables) throws Exception {
		String res = "";

		if (value.getMaximumCardinality() == DBMRole.N_CARD) {
			// list, set, ordered set
			String type = value.getMetaPropertyStringValue(Constants.HYDRA_TYPE_MP);
			if (type == null || type.trim().isEmpty())
				type = Constants.LIST_TYPE;

			if (value.getMetaPropertyStringListValue(Constants.STEREOTYPE) != null
					&& value.getMetaPropertyStringListValue(Constants.STEREOTYPE).contains(Constants.COMPOSED_FIELD)) {
				// composed field e.g., list { x: "VALUE"[y]}
				return type + "{ " + readComposedField((DBMSimpleAttribute) value, variables) + " }";
			} else {
				// normal field e.g., list {x}
				return type + " { " + value.getName() + " }";
			}

		} else if (value instanceof DBMCompoundAttribute) {
			// hash

			res = "hash {\n";
			DBMAttribute a = ((DBMCompoundAttribute) value).getFirstAttribute();
			int cnt = 0;
			while (a != null) {
				if (cnt > 0)
					res += ",\n";
				if (a.getMetaPropertyStringListValue(Constants.STEREOTYPE) == null
						|| !a.getMetaPropertyStringListValue(Constants.STEREOTYPE).contains(Constants.COMPOSED_FIELD)) {
					// normal column
					res += TAB(4) + a.getName();
				} else {
					// composed field
					String composedField = readComposedField((DBMSimpleAttribute) a, variables);
					res += TAB(4) + composedField;
				}

				cnt++;

				a = ((DBMCompoundAttribute) value).getNextAttribute(a);
			}

			res += "\n" + TAB(3) + "}";

		} else {
			// binary value
			if (value.getMetaPropertyStringListValue(Constants.STEREOTYPE) != null
					&& value.getMetaPropertyStringListValue(Constants.STEREOTYPE).contains(Constants.COMPOSED_FIELD)) {
				// composed field e.g., x: "VALUE"[y]
				res = readComposedField((DBMSimpleAttribute) value, variables);
			} else {
				// normal field e.g., list {x}
				res = value.getName();
			}
		}

		return res;
	}

	private DBMAttribute getRedisKey(DBMEntityType ent) throws Exception {
		DBMAttribute res = null;
		DBMAttribute a = ent.getFirstAttribute();
		while (a != null) {
			if (a.getMetaPropertyStringListValue(Constants.STEREOTYPE) != null
					&& a.getMetaPropertyStringListValue(Constants.STEREOTYPE).contains(Constants.KEY)) {
				if (res != null)
					throw new Exception("There are more than on keys defined in kvpairs: " + ent.getName());
				res = a;
			}
			a = ent.getNextAttribute(a);
		}
		return res;
	}

	private DBMAttribute getRedisValue(DBMEntityType ent) throws Exception {
		DBMAttribute res = null;
		DBMAttribute a = ent.getFirstAttribute();
		while (a != null) {
			if (a.getMetaPropertyStringListValue(Constants.STEREOTYPE) != null
					&& a.getMetaPropertyStringListValue(Constants.STEREOTYPE).contains(Constants.VALUE)) {
				if (res != null)
					throw new Exception("There are more than one values defined in kvpairs: " + ent.getName());
				res = a;
			}
			a = ent.getNextAttribute(a);
		}
		return res;
	}

	private void readDocumentSchema(DBMCollection schema) throws Exception {
		DBMEntityType ent = schema.getFirstDataObjectEntityType();
		while (ent != null) {
			physicalSchemaStr
					.append(TAB + TAB + "collection " + ent.getName() + " {\n" + TAB + TAB + TAB + "fields {\n");
			Map<DBMSimpleAttribute, Integer> variables = new HashMap<DBMSimpleAttribute, Integer>();
			DBMSimpleAttribute attr = ent.getFirstAttributeSimpleAttribute();
			while (attr != null) {
				if (attr.getMetaPropertyStringListValue(Constants.STEREOTYPE) != null
						&& attr.getMetaPropertyStringListValue(Constants.STEREOTYPE).contains(Constants.VARIABLE)) {
					variables.put(attr, variables.size() + 1);
				}
				attr = ent.getNextAttributeSimpleAttribute(attr);
			}

			DBMAttribute a = ent.getFirstAttribute();
			int cnt = 0;
			while (a != null) {
				if (cnt > 0)
					physicalSchemaStr.append(",\n");

				boolean ok = readDocumentAttribute(a, variables, 4);
				a = ent.getNextAttribute(a);
				if (ok)
					cnt++;
			}

			physicalSchemaStr.append("\n" + TAB(3) + "}\n");

			Map<DBMGroup, DBMGroup> references = new LinkedHashMap<DBMGroup, DBMGroup>();
			DBMGroup g = ent.getFirstGroup();
			while (g != null) {
				if (g.getFirstConstraintOrigin() != null) {
					DBMGroup targetGrp = g.getFirstConstraintOrigin().getFirstConstraintMember().getGroup();
					references.put(g, targetGrp);
				}
				g = ent.getNextGroup(g);
			}

			if (references.size() > 0) {
				int randomNameCnt = 1;
				physicalSchemaStr.append("\n" + TAB(3) + "references {\n");
				for (Entry<DBMGroup, DBMGroup> entry : references.entrySet()) {
					DBMGroup origin = entry.getKey();
					DBMGroup target = entry.getValue();
					String refName = origin.getName();
					if (refName == null || refName.trim().isEmpty()) {
						// random ref name
						refName = "ref" + randomNameCnt;
						randomNameCnt++;
					}

					String referenceStr = refName + ": ";
					DBMGenericObject comp = origin.getFirstComponent();
					int cnt2 = 0;
					while (comp != null) {
						if (cnt2 > 0)
							referenceStr += ", ";
						a = (DBMAttribute) comp;
						referenceStr += getEmbeddedAttributeName(a);
						comp = origin.getNextComponent(comp);
						cnt2++;
					}

					referenceStr += " -> ";

					cnt2 = 0;
					comp = target.getFirstComponent();
					while (comp != null) {
						if (cnt2 > 0)
							referenceStr += ", ";
						a = (DBMAttribute) comp;
						referenceStr += getAbsoluteEmbeddedAttributeName(a);
						comp = target.getNextComponent(comp);
						cnt2++;
					}

					physicalSchemaStr.append(TAB(4) + referenceStr + "\n");
				}
				physicalSchemaStr.append(TAB(3) + "}\n");
			}

			physicalSchemaStr.append(TAB + TAB + "}\n\n");
			ent = schema.getNextDataObjectEntityType(ent);
		}
	}

	private void readRelationSchema(DBMCollection schema) throws Exception {
		DBMEntityType ent = schema.getFirstDataObjectEntityType();
		while (ent != null) {
			physicalSchemaStr.append(TAB + TAB + "table " + ent.getName() + " {\n" + TAB + TAB + TAB + "columns {\n");
			Map<DBMSimpleAttribute, Integer> variables = new HashMap<DBMSimpleAttribute, Integer>();
			DBMSimpleAttribute attr = ent.getFirstAttributeSimpleAttribute();
			while (attr != null) {
				if (attr.getMetaPropertyStringListValue(Constants.STEREOTYPE) != null
						&& attr.getMetaPropertyStringListValue(Constants.STEREOTYPE).contains(Constants.VARIABLE)) {
					variables.put(attr, variables.size() + 1);
				}
				attr = ent.getNextAttributeSimpleAttribute(attr);
			}

			attr = ent.getFirstAttributeSimpleAttribute();
			int cnt = 0;
			while (attr != null) {
				if (attr.getMetaPropertyStringListValue(Constants.STEREOTYPE) == null
						|| !attr.getMetaPropertyStringListValue(Constants.STEREOTYPE).contains(Constants.VARIABLE)) {
					if (cnt > 0)
						physicalSchemaStr.append(",\n");
					if (attr.getMetaPropertyStringListValue(Constants.STEREOTYPE) == null || !attr
							.getMetaPropertyStringListValue(Constants.STEREOTYPE).contains(Constants.COMPOSED_FIELD)) {
						// normal column
						physicalSchemaStr.append(TAB(4) + attr.getName());
					} else {
						// composed field
						String composedField = readComposedField(attr, variables);
						physicalSchemaStr.append(TAB(4) + composedField);
					}

					cnt++;
				}
				attr = ent.getNextAttributeSimpleAttribute(attr);
			}
			physicalSchemaStr.append("\n" + TAB(3) + "}\n");

			Map<DBMGroup, DBMGroup> references = new LinkedHashMap<DBMGroup, DBMGroup>();
			DBMGroup g = ent.getFirstGroup();
			while (g != null) {
				if (g.getFirstConstraintOrigin() != null) {
					DBMGroup targetGrp = g.getFirstConstraintOrigin().getFirstConstraintMember().getGroup();
					references.put(g, targetGrp);
				}
				g = ent.getNextGroup(g);
			}

			if (references.size() > 0) {
				int randomNameCnt = 1;
				physicalSchemaStr.append("\n" + TAB(3) + "references {\n");
				for (Entry<DBMGroup, DBMGroup> entry : references.entrySet()) {
					DBMGroup origin = entry.getKey();
					DBMGroup target = entry.getValue();
					String refName = origin.getName();
					if (refName == null || refName.trim().isEmpty()) {
						// random ref name
						refName = "ref" + randomNameCnt;
						randomNameCnt++;
					}

					String referenceStr = refName + ": ";
					DBMGenericObject comp = origin.getFirstComponent();
					int cnt2 = 0;
					while (comp != null) {
						if (cnt2 > 0)
							referenceStr += ", ";
						DBMAttribute a = (DBMAttribute) comp;
						referenceStr += getEmbeddedAttributeName(a);
						comp = origin.getNextComponent(comp);
						cnt2++;
					}

					referenceStr += " -> ";

					cnt2 = 0;
					comp = target.getFirstComponent();
					while (comp != null) {
						if (cnt2 > 0)
							referenceStr += ", ";
						DBMAttribute a = (DBMAttribute) comp;
						referenceStr += getAbsoluteEmbeddedAttributeName(a);
						comp = target.getNextComponent(comp);
						cnt2++;
					}

					physicalSchemaStr.append(TAB(4) + referenceStr + "\n");
				}
				physicalSchemaStr.append(TAB(3) + "}\n");
			}

			physicalSchemaStr.append(TAB + TAB + "}\n\n");
			ent = schema.getNextDataObjectEntityType(ent);
		}

	}

	private String getEmbeddedAttributeName(DBMAttribute a) {
		String res = a.getName();
		DBMGenericObject parent = a.getAttributeOwner();
		if (parent instanceof DBMAttribute)
			res = getEmbeddedAttributeName((DBMAttribute) parent) + "." + res;

		return res;
	}

	private String getAbsoluteEmbeddedAttributeName(DBMAttribute a) throws Exception {
		String res = a.getName();
		DBMGenericObject parent = a.getAttributeOwner();
		if (parent instanceof DBMAttribute)
			res = getEmbeddedAttributeName((DBMAttribute) parent) + "." + res;
		if (parent instanceof DBMEntityType)
			res = getPhysicalSchema((DBMEntityType) parent).getName() + "." + ((DBMEntityType) parent).getName() + "."
					+ res;

		return res;
	}

	private DBMCollection getPhysicalSchema(DBMEntityType parent) throws Exception {
		DBMCollection res = null;
		DBMSchema sch = parent.getSchema();
		DBMCollection coll = sch.getFirstCollection();
		while (coll != null) {
			DBMEntityType e = coll.getFirstDataObjectEntityType();
			while (e != null) {
				if (e.equals(parent))
					if (res != null)
						throw new Exception(
								"Entity type " + parent.getName() + " is declared in more than one physical schemas");
					else
						res = coll;
				e = coll.getNextDataObjectEntityType(e);
			}
			coll = sch.getNextCollection(coll);
		}

		if (res == null)
			throw new Exception("Entity type " + parent.getName() + " is not declared in any physical schemas");

		return res;
	}

	private String readComposedField(DBMSimpleAttribute attr, Map<DBMSimpleAttribute, Integer> variables)
			throws Exception {
		String fullName = attr.getName();
		String regex = "([^:]+)(:)(.+)";
		Pattern p = Pattern.compile(regex);
		Matcher m = p.matcher(fullName);

		if (m.matches()) {
			String composedFieldName = m.group(1).trim();
			String composition = m.group(3).trim();

			for (Entry<DBMSimpleAttribute, Integer> entry : variables.entrySet()) {
				DBMSimpleAttribute a = entry.getKey();
				int nb = entry.getValue();
				composition = composition.replaceAll("\\[" + nb + "\\]", "[" + a.getName() + "]");
			}

			return composedFieldName + ": " + composition;

		} else {
			throw new Exception("Composed field with invalid name: " + fullName);
		}
	}

	private String readArrayField(DBMSimpleAttribute attr, Map<DBMSimpleAttribute, Integer> variables)
			throws Exception {
		String fullName = attr.getName();
		String regex = "([^:]+)(:)(.+)";
		Pattern p = Pattern.compile(regex);
		Matcher m = p.matcher(fullName);

		if (m.matches()) {
			String composedFieldName = m.group(1).trim();
			String composition = m.group(3).trim();

			for (Entry<DBMSimpleAttribute, Integer> entry : variables.entrySet()) {
				DBMSimpleAttribute a = entry.getKey();
				int nb = entry.getValue();
				composition = composition.replaceAll("\\[" + nb + "\\]", a.getName());
			}

			return composedFieldName + "[]: " + composition;

		} else {
			throw new Exception("Composed field with invalid name: " + fullName);
		}
	}

	private void readDatabases() throws Exception {
		databasesStr.append("databases {\n");
		DBMCollection db = dbSchema.getFirstCollection();
		while (db != null) {
			String dbType = db.getMetaPropertyStringValue(Constants.DB_TYPE_MP);
			if (dbType == null || dbType.trim().isEmpty())
				throw new Exception("Database " + db.getName() + " has no defined db type");

			databasesStr.append(TAB(1) + dbType + " " + db.getName() + " {\n");

			String dbName = db.getMetaPropertyStringValue(Constants.DB_NAME_MP);
			String host = db.getMetaPropertyStringValue(Constants.DB_HOST_MP);
			Integer port = db.getMetaPropertyIntValue(Constants.DB_PORT_MP);
			String login = db.getMetaPropertyStringValue(Constants.DB_LOGIN_MP);
			String password = db.getMetaPropertyStringValue(Constants.DB_PASSWORD_MP);

			if (dbName != null && !dbName.isEmpty()) {
				databasesStr.append(TAB(2) + "dbname: \"" + dbName + "\"\n");
			}

			if (host != null && !host.isEmpty()) {
				databasesStr.append(TAB(2) + "host: \"" + host + "\"\n");
			}

			if (port != null && !port.toString().isEmpty()) {
				databasesStr.append(TAB(2) + "port: " + port + "\n");
			}

			if (login != null && !login.isEmpty()) {
				databasesStr.append(TAB(2) + "login: \"" + login + "\"\n");
			}

			if (password != null && !password.isEmpty()) {
				databasesStr.append(TAB(2) + "password: \"" + password + "\"\n");
			}

			databasesStr.append(TAB(1) + "}\n");
			db = dbSchema.getNextCollection(db);
		}
		databasesStr.append("\n}");
	}

	private void readMappings() throws Exception {
		mappingsStr.append("mapping rules {\n");

		DBMEntityType ent = conceptualSchema.getFirstDataObjectEntityType();
		boolean atLeastOneRule = false;
		while (ent != null) {
			// Entity Mapping rules
			Map<DBMAttribute, List<DBMAttribute>> mappings = getEntityOrRelationshipTypeMappingRule(ent);
			Map<DBMAttributeOwner, Map<DBMAttribute, List<DBMAttribute>>> mappingByAttributeOwner = sortByAttributeOwner(
					mappings);
			atLeastOneRule = generateEntityOrRelationMappingRules(ent, mappingByAttributeOwner, atLeastOneRule);

			ent = conceptualSchema.getNextDataObjectEntityType(ent);
		}
		
		
		//RelationshipMappingRule
		DBMRelationshipType rel = conceptualSchema.getFirstDataObjectRelationshipType();
		while(rel != null) {
			Map<DBMAttribute, List<DBMAttribute>> mappings = getEntityOrRelationshipTypeMappingRule(rel);
			Map<DBMAttributeOwner, Map<DBMAttribute, List<DBMAttribute>>> mappingByAttributeOwner = sortByAttributeOwner(
					mappings);
			atLeastOneRule = generateEntityOrRelationMappingRules(rel, mappingByAttributeOwner, atLeastOneRule);
			
			rel = conceptualSchema.getNextDataObjectRelationshipType(rel);
		}
		
		//RoleToEmbbededObjectMappingRule
		rel = conceptualSchema.getFirstDataObjectRelationshipType();
		while(rel != null) {
			DBMRole role = rel.getFirstRole();
			while(role != null) {
				List<DBMCompoundAttribute> embeddedFields = getMappedEmbeddedFields(role);
				atLeastOneRule = generateRoleToEmbeddedObjectMappingRules(role, embeddedFields, atLeastOneRule);
				role = rel.getNextRole(role);
			}
			rel = conceptualSchema.getNextDataObjectRelationshipType(rel);
		}
		
		//RoleToReferenceMappingRule
		rel = conceptualSchema.getFirstDataObjectRelationshipType();
		while(rel != null) {
			DBMRole role = rel.getFirstRole();
			while(role != null) {
				List<DBMGroup> groups = getMappedReferenceGroup(role);
				atLeastOneRule = generateRoleToReferenceMappingRules(role, groups, atLeastOneRule);
				role = rel.getNextRole(role);
			}
			rel = conceptualSchema.getNextDataObjectRelationshipType(rel);
		}
		
		//TODO ? RoleToKeyBracketsFieldMappingRule
		// none of the developers can tell what a RoleToKeyBracketsFieldMappingRule is

		mappingsStr.append("\n}\n");
	}
	
	private boolean generateRoleToEmbeddedObjectMappingRules(DBMRole role, List<DBMCompoundAttribute> embeddedFields,
			boolean atLeastOneRule) throws Exception {
		String conceptualPart = conceptualSchemaName + "." + role.getRelationshipType().getName() + "." + role.getName() + " -> ";
		for(DBMCompoundAttribute attr : embeddedFields) {
			String rule = conceptualPart + getAbsoluteEmbeddedAttributeName(attr) + "()";
			if(atLeastOneRule)
				mappingsStr.append(",\n");
			
			mappingsStr.append(TAB(1) + rule);
			atLeastOneRule = true;
		}
		
		return atLeastOneRule;
	}
	
	private boolean generateRoleToReferenceMappingRules(DBMRole role, List<DBMGroup> references,
			boolean atLeastOneRule) throws Exception {
		String conceptualPart = conceptualSchemaName + "." + role.getRelationshipType().getName() + "." + role.getName() + " -> ";
		for(DBMGroup g : references) {
			DBMEntityType ent = (DBMEntityType) g.getDataObject();
			DBMCollection coll = getPhysicalSchema(ent);
			String structName = coll.getName() + "." + ent.getName() + "." + g.getName();
			String rule = conceptualPart + structName;
			if(atLeastOneRule)
				mappingsStr.append(",\n");
			
			mappingsStr.append(TAB(1) + rule);
			atLeastOneRule = true;
		}
		
		return atLeastOneRule;
	}
	
	private List<DBMCompoundAttribute> getMappedEmbeddedFields(DBMRole role) {
		List<DBMCompoundAttribute> res = new ArrayList<DBMCompoundAttribute>();
		DBMCollection coll = physicalSchema.getFirstCollection();
		while(coll != null) {
			DBMEntityType ent = coll.getFirstDataObjectEntityType();
			while(ent != null) {
				res.addAll(getMappedEmbeddedFields(role, ent));
				ent = coll.getNextDataObjectEntityType(ent);
			}
			coll = physicalSchema.getNextCollection(coll);
		}
		return res;
	}
	
	private List<DBMGroup> getMappedReferenceGroup(DBMRole role) {
		List<DBMGroup> res = new ArrayList<DBMGroup>();
		DBMCollection coll = physicalSchema.getFirstCollection();
		while(coll != null) {
			DBMEntityType ent = coll.getFirstDataObjectEntityType();
			while(ent != null) {
				DBMGroup g = ent.getFirstGroup();
				while (g != null) {
					if (g.getFirstConstraintOrigin() != null && isMapped(role, g)) {
						res.add(g);
					}
					g = ent.getNextGroup(g);
				}
				
				
				ent = coll.getNextDataObjectEntityType(ent);
			}
			coll = physicalSchema.getNextCollection(coll);
		}
		return res;
	}
	

	private List<DBMCompoundAttribute> getMappedEmbeddedFields(DBMRole role, DBMAttributeOwner owner) {
		List<DBMCompoundAttribute> res = new ArrayList<DBMCompoundAttribute>();
		if(owner instanceof DBMCompoundAttribute && isMapped((DBMGenericObject) owner, role))
			res.add((DBMCompoundAttribute) owner);
		
		DBMAttribute a = owner.getFirstAttribute();
		while(a != null) {
			if(a instanceof DBMAttributeOwner)
				res.addAll(getMappedEmbeddedFields(role, (DBMAttributeOwner) a));
			a = owner.getNextAttribute(a);
		}
		
		return res;
	}

	private boolean generateEntityOrRelationMappingRules(DBMEntityRelationshipType ent,
			Map<DBMAttributeOwner, Map<DBMAttribute, List<DBMAttribute>>> mappingByAttributeOwner, boolean atLeastOneRule) throws Exception {

		String conceptualPart = conceptualSchemaName + "." + ent.getName() + "(";
		for (Entry<DBMAttributeOwner, Map<DBMAttribute, List<DBMAttribute>>> entry : mappingByAttributeOwner
				.entrySet()) {
			DBMAttributeOwner owner = entry.getKey();
			Map<DBMAttribute, List<DBMAttribute>> attributes = entry.getValue();

			String rule = conceptualPart;
			int i = 0;
			for (DBMAttribute a : attributes.keySet()) {
				if (i > 0)
					rule += ", ";
				rule += a.getName();
				i++;
			}

			rule += ") -> ";

			String physicalOwner = "";
			if (owner instanceof DBMAttribute) {
				physicalOwner = getAbsoluteEmbeddedAttributeName((DBMAttribute) owner);
			}
			if (owner instanceof DBMEntityType) {
				physicalOwner = getPhysicalSchema((DBMEntityType) owner) + "." + ((DBMEntityType) owner).getName();
			}

			String staticOwner = physicalOwner;

			physicalOwner += "(";
			int j = 0;
			for (DBMAttribute a : attributes.keySet()) {
				if (j > 0)
					physicalOwner += ", ";

				List<DBMAttribute> pfields = attributes.get(a);
				physicalOwner += pfields.get(0).getName();

				pfields.remove(0);

				if (pfields.size() > 0) {
					// more than one fields in same owner mapped to the same conceptual attribute.
					// E.g, table Customer (lastname, lastname2)
					// separate rules are required
					for (DBMAttribute a2 : pfields) {
						String rule2 = conceptualPart + a.getName() + ") -> " + staticOwner + "(" + a2.getName() + ")";
						if(atLeastOneRule)
							mappingsStr.append(",\n");
						
						mappingsStr.append(TAB(1) + (ent instanceof DBMRelationshipType ? "rel: " : "") + rule2);
						atLeastOneRule = true;
					}
				}

				j++;
			}

			physicalOwner += ")";

			rule += physicalOwner;
			if(atLeastOneRule)
				mappingsStr.append(",\n");
			
			mappingsStr.append(TAB(1) + (ent instanceof DBMRelationshipType ? "rel: " : "") + rule);
			atLeastOneRule = true;
		}
		
		return atLeastOneRule;

	}

	private Map<DBMAttributeOwner, Map<DBMAttribute, List<DBMAttribute>>> sortByAttributeOwner(
			Map<DBMAttribute, List<DBMAttribute>> mappings) {
		Map<DBMAttributeOwner, Map<DBMAttribute, List<DBMAttribute>>> res = new HashMap<DBMAttributeOwner, Map<DBMAttribute, List<DBMAttribute>>>();

		for (Entry<DBMAttribute, List<DBMAttribute>> entry : mappings.entrySet()) {
			DBMAttribute attr = entry.getKey();
			List<DBMAttribute> fields = entry.getValue();

			for (DBMAttribute field : fields) {
				DBMAttributeOwner parent = (DBMAttributeOwner) field.getAttributeOwner();
				Vector<String> stereotypes = ((DBMGenericObject) parent).getMetaPropertyStringListValue(Constants.STEREOTYPE);
				if(stereotypes != null && stereotypes.contains(Constants.VALUE) && parent instanceof DBMCompoundAttribute) {
					//parent = hash {...}
					parent = (DBMAttributeOwner) ((DBMCompoundAttribute)parent).getAttributeOwner();
				}
				
				
				Map<DBMAttribute, List<DBMAttribute>> map = res.get(parent);
				if (map == null) {
					map = new LinkedHashMap<DBMAttribute, List<DBMAttribute>>();
					res.put(parent, map);
				}

				List<DBMAttribute> pfields = map.get(attr);
				if (pfields == null) {
					pfields = new ArrayList<DBMAttribute>();
					map.put(attr, pfields);
				}
				pfields.add(field);
			}

		}

		return res;
	}

	private Map<DBMAttribute, List<DBMAttribute>> getEntityOrRelationshipTypeMappingRule(DBMEntityRelationshipType ent) {
		Map<DBMAttribute, List<DBMAttribute>> res = new LinkedHashMap<DBMAttribute, List<DBMAttribute>>();

		DBMAttribute a = ent.getFirstAttribute();
		while (a != null) {
			DBMEntityType physicalStruc = physicalSchema.getFirstDataObjectEntityType();
			while (physicalStruc != null) {
				List<DBMAttribute> fields = getEntityMappingRule(a, physicalStruc);
				if (fields != null && fields.size() > 0) {
					List<DBMAttribute> list = res.get(a);
					if (list == null) {
						list = new ArrayList<DBMAttribute>();
						res.put(a, list);
					}

					list.addAll(fields);
				}

				physicalStruc = physicalSchema.getNextDataObjectEntityType(physicalStruc);
			}

			a = ent.getNextAttribute(a);
		}

		return res;

	}

	private List<DBMAttribute> getEntityMappingRule(DBMAttribute a, DBMAttributeOwner owner) {
		List<DBMAttribute> res = new ArrayList<DBMAttribute>();
		DBMAttribute a2 = owner.getFirstAttribute();
		while (a2 != null) {
			if (isMapped(a, a2))
				res.add(a2);

			if (a2 instanceof DBMCompoundAttribute) {
				res.addAll(getEntityMappingRule(a, (DBMAttributeOwner) a2));
			}

			a2 = owner.getNextAttribute(a2);
		}
		return res;
	}

	private boolean isMapped(DBMGenericObject a, DBMGenericObject a2) {
		Vector<Integer> oids1 = a.getMetaPropertyIntListValue(Constants.MAPPING_OID_MP);
		Vector<Integer> oids2 = a2.getMetaPropertyIntListValue(Constants.MAPPING_OID_MP);
		if (oids1 == null || oids2 == null)
			return false;

		for (Integer oid : oids1)
			if (oids2.contains(oid))
				return true;
		return false;
	}

	private String getDatabasesMappings(DBMCollection schema) {
		String res = "";
		Vector<Integer> oids = schema.getMetaPropertyIntListValue(Constants.MAPPING_OID_MP);

		DBMCollection db = dbSchema.getFirstCollection();
		int cnt = 0;
		while (db != null) {
			Vector<Integer> oids2 = db.getMetaPropertyIntListValue(Constants.MAPPING_OID_MP);
			if (inter(oids, oids2)) {
				if (cnt == 0)
					res = " : ";
				else
					res += ", ";
				res += db.getName();
				cnt++;
			}

			db = dbSchema.getNextCollection(db);
		}

		return res;

	}

	private boolean inter(Vector<Integer> oids, Vector<Integer> oids2) {
		if (oids == null || oids2 == null)
			return false;
		for (Integer oid : oids)
			for (Integer oid2 : oids2)
				if (oid.intValue() == oid2.intValue())
					return true;
		return false;
	}

	private String getRoleStr(DBMRole role) {
		if (role.getMinimumCardinality() == 0 && role.getMaximumCardinality() == 1)
			return "[0-1]";
		if (role.getMinimumCardinality() == 1 && role.getMaximumCardinality() == 1)
			return "[1]";
		if (role.getMinimumCardinality() == 0 && role.getMaximumCardinality() == DBMRole.N_CARD)
			return "[0-N]";
		if (role.getMinimumCardinality() == 1 && role.getMaximumCardinality() == DBMRole.N_CARD)
			return "[1-N]";
		return "[?]";
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

	private String TAB(int nb) {
		String res = "";
		for (int i = 0; i < nb; i++)
			res += TAB;
		return res;
	}

}
