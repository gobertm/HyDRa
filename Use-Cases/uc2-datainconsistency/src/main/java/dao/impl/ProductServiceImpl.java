package dao.impl;
import exceptions.PhysicalStructureException;
import java.util.Arrays;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import pojo.Product;
import conditions.*;
import dao.services.ProductService;
import util.Util;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.api.java.function.MapFunction;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import com.mongodb.spark.MongoSpark;
import org.bson.Document;
import static java.util.Collections.singletonList;
import dbconnection.SparkConnectionMgr;
import dbconnection.DBConnectionMgr;
import scala.collection.mutable.WrappedArray;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FilterFunction;
import java.util.ArrayList;
import org.apache.commons.lang.mutable.MutableBoolean;
import tdo.*;
import pojo.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.ArrayType;
import scala.Tuple2;
import org.bson.Document;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;


public class ProductServiceImpl extends ProductService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProductServiceImpl.class);
	
	
	
	
	public static String getBSONMatchQueryInCategoryCollectionFromMymongo2(Condition<ProductAttribute> condition, MutableBoolean refilterFlag) {	
		String res = null;	
		if(condition != null) {
			if(condition instanceof SimpleCondition) {
				ProductAttribute attr = ((SimpleCondition<ProductAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<ProductAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<ProductAttribute>) condition).getValue();
				if(value != null) {
					String valueString = Util.transformBSONValue(value);
					boolean isConditionAttrEncountered = false;
	
					if(attr == ProductAttribute.id ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "id': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "products." + res;
					res = "'" + res;
					}
					if(attr == ProductAttribute.Name ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "name': {" + mongoOp + ": " + preparedValue + "}";
	
						res = "products." + res;
					res = "'" + res;
					}
					if(attr == ProductAttribute.category ) {
						isConditionAttrEncountered = true;
					
						String mongoOp = op.getMongoDBOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "'.*" + Util.escapeReservedRegexMongo(valueString)  + ".*'";
						} else {
							preparedValue = Util.getDelimitedMongoValue(value.getClass(), preparedValue);
						}
						res = "categoryname': {" + mongoOp + ": " + preparedValue + "}";
	
					res = "'" + res;
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						res = "$expr: {$eq:[1,1]}";
					}
					
				}
			}
	
			if(condition instanceof AndCondition) {
				String bsonLeft = getBSONMatchQueryInCategoryCollectionFromMymongo2(((AndCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInCategoryCollectionFromMymongo2(((AndCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $and: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";
			}
	
			if(condition instanceof OrCondition) {
				String bsonLeft = getBSONMatchQueryInCategoryCollectionFromMymongo2(((OrCondition)condition).getLeftCondition(), refilterFlag);
				String bsonRight = getBSONMatchQueryInCategoryCollectionFromMymongo2(((OrCondition)condition).getRightCondition(), refilterFlag);			
				if(bsonLeft == null && bsonRight == null)
					return null;
				if(bsonLeft == null)
					return bsonRight;
				if(bsonRight == null)
					return bsonLeft;
				res = " $or: [ {" + bsonLeft + "}, {" + bsonRight + "}] ";	
			}
	
			
	
			
		}
	
		return res;
	}
	
	public Dataset<Product> getProductListInCategoryCollectionFromMymongo2(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag){
		String bsonQuery = ProductServiceImpl.getBSONMatchQueryInCategoryCollectionFromMymongo2(condition, refilterFlag);
		if(bsonQuery != null) {
			bsonQuery = "{$match: {" + bsonQuery + "}}";	
		} 
		
		Dataset<Row> dataset = dbconnection.SparkConnectionMgr.getSparkSessionForMongoDB("mymongo2", "categoryCollection", bsonQuery);
	
		Dataset<Product> res = dataset.flatMap((FlatMapFunction<Row, Product>) r -> {
				List<Product> list_res = new ArrayList<Product>();
				Integer groupIndex = null;
				String regex = null;
				String value = null;
				Pattern p = null;
				Matcher m = null;
				boolean matches = false;
				Row nestedRow = null;
	
				boolean addedInList = false;
				Row r1 = r;
				Product product1 = new Product();
					boolean toAdd1  = false;
					WrappedArray array1  = null;
					// 	attribute Product.category for field categoryname			
					nestedRow =  r1;
					if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("categoryname")) {
						if(nestedRow.getAs("categoryname")==null)
							product1.setCategory(null);
						else{
							product1.setCategory((String) nestedRow.getAs("categoryname"));
							toAdd1 = true;					
							}
					}
					array1 = r1.getAs("products");
					if(array1!= null) {
						for (int i2 = 0; i2 < array1.size(); i2++){
							Row r2 = (Row) array1.apply(i2);
							Product product2 = (Product) product1.clone();
							boolean toAdd2  = false;
							WrappedArray array2  = null;
							// 	attribute Product.id for field id			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("id")) {
								if(nestedRow.getAs("id")==null)
									product2.setId(null);
								else{
									product2.setId((String) nestedRow.getAs("id"));
									toAdd2 = true;					
									}
							}
							// 	attribute Product.Name for field name			
							nestedRow =  r2;
							if(nestedRow != null && Arrays.asList(nestedRow.schema().fieldNames()).contains("name")) {
								if(nestedRow.getAs("name")==null)
									product2.setName(null);
								else{
									product2.setName((String) nestedRow.getAs("name"));
									toAdd2 = true;					
									}
							}
							if(toAdd2) {
								if(condition ==null || refilterFlag.booleanValue() || condition.evaluate(product2))
								list_res.add(product2);
								addedInList = true;
							} 
							if(addedInList)
								toAdd1 = false;
						}
					}
					
					if(toAdd1) {
						
							list_res.add(product1);
						addedInList = true;
					} 
					
					
				
				return list_res.iterator();
	
		}, Encoders.bean(Product.class));
		res= res.dropDuplicates(new String[]{"id"});
		return res;
		
	}
	
	
	
	//TODO redis
	public Dataset<Product> getProductListInKVProdPriceFromMyredis(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag){
		// Build the key pattern
		//  - If the condition attribute is in the key pattern, replace by the value. Only if operator is EQUALS.
		//  - Replace all other fields of key pattern by a '*' 
		String keypattern= "", keypatternAllVariables="";
		String valueCond=null;
		String finalKeypattern;
		List<String> fieldsListInKey = new ArrayList<>();
		Set<ProductAttribute> keyAttributes = new HashSet<>();
		keypattern=keypattern.concat("PRODUCT:");
		keypatternAllVariables=keypatternAllVariables.concat("PRODUCT:");
		if(!Util.containsOrCondition(condition)){
			valueCond=Util.getStringValue(Util.getValueOfAttributeInEqualCondition(condition,ProductAttribute.id));
			keyAttributes.add(ProductAttribute.id);
		}
		else{
			valueCond=null;
			refilterFlag.setValue(true);
		}
		if(valueCond==null)
			keypattern=keypattern.concat("*");
		else
			keypattern=keypattern.concat(valueCond);
		fieldsListInKey.add("prodID");
		keypatternAllVariables=keypatternAllVariables.concat("*");
		keypattern=keypattern.concat(":PRICE");
		keypatternAllVariables=keypatternAllVariables.concat(":PRICE");
		if(!refilterFlag.booleanValue()){
			Set<ProductAttribute> conditionAttributes = Util.getConditionAttributes(condition);
			for (ProductAttribute a : conditionAttributes) {
				if (!keyAttributes.contains(a)) {
					refilterFlag.setValue(true);
					break;
				}
			}
		}
			
		// Find the type of query to perform in order to retrieve a Dataset<Row>
		// Based on the type of the value. Is a it a simple string or a hash or a list... 
		Dataset<Row> rows;
		rows = SparkConnectionMgr.getRowsFromKeyValue("myredis",keypattern);
		// Transform to POJO. Based on Row containing (String key, String value)
		finalKeypattern = keypatternAllVariables;
		Dataset<Product> res = rows.map((MapFunction<Row, Product>) r -> {
					Product product_res = new Product();
					Integer groupindex = null;
					String regex = null;
					String value = null;
					Pattern p, pattern = null;
					Matcher m, match = null;
					String key="";
					boolean matches = false;
					// attribute [Product.Id]
					// Attribute mapped in a key.
					key = r.getAs("key");
					regex = finalKeypattern.replaceAll("\\*","(.*)");
					groupindex = fieldsListInKey.indexOf("prodID")+1;
					if(groupindex==null) {
						logger.warn("Attribute of 'Product' mapped physical field 'prodID' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
					}
					p = Pattern.compile(regex);
					m = p.matcher(key);
					matches = m.find();
					String id = null;
					if(matches) {
						id = m.group(groupindex.intValue());
					} else {
						logger.warn("Cannot retrieve value for Productid attribute stored in db myredis. Probably due to an ambiguous regex.");
						product_res.addLogEvent("Cannot retrieve value for Product.id attribute stored in db myredis. Probably due to an ambiguous regex.");
					}
					product_res.setId(id == null ? null : id);
					// attribute [Product.Price]
					// Attribute mapped in value part.
					value = r.getAs("value");
					Integer price = value == null ? null : Integer.parseInt(value);
					product_res.setPrice(price);
					
						
					return product_res;
				}, Encoders.bean(Product.class));
		if(refilterFlag.booleanValue())
			res = res.filter((FilterFunction<Product>) r -> condition == null || condition.evaluate(r));
		res=res.dropDuplicates();
		return res;
		
	}
	
	
	
	//TODO redis
	public Dataset<Product> getProductListInKVProdPhotosFromMyredis(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag){
		// Build the key pattern
		//  - If the condition attribute is in the key pattern, replace by the value. Only if operator is EQUALS.
		//  - Replace all other fields of key pattern by a '*' 
		String keypattern= "", keypatternAllVariables="";
		String valueCond=null;
		String finalKeypattern;
		List<String> fieldsListInKey = new ArrayList<>();
		Set<ProductAttribute> keyAttributes = new HashSet<>();
		keypattern=keypattern.concat("PRODUCT:");
		keypatternAllVariables=keypatternAllVariables.concat("PRODUCT:");
		if(!Util.containsOrCondition(condition)){
			valueCond=Util.getStringValue(Util.getValueOfAttributeInEqualCondition(condition,ProductAttribute.id));
			keyAttributes.add(ProductAttribute.id);
		}
		else{
			valueCond=null;
			refilterFlag.setValue(true);
		}
		if(valueCond==null)
			keypattern=keypattern.concat("*");
		else
			keypattern=keypattern.concat(valueCond);
		fieldsListInKey.add("prodID");
		keypatternAllVariables=keypatternAllVariables.concat("*");
		keypattern=keypattern.concat(":PHOTO");
		keypatternAllVariables=keypatternAllVariables.concat(":PHOTO");
		if(!refilterFlag.booleanValue()){
			Set<ProductAttribute> conditionAttributes = Util.getConditionAttributes(condition);
			for (ProductAttribute a : conditionAttributes) {
				if (!keyAttributes.contains(a)) {
					refilterFlag.setValue(true);
					break;
				}
			}
		}
			
		// Find the type of query to perform in order to retrieve a Dataset<Row>
		// Based on the type of the value. Is a it a simple string or a hash or a list... 
		Dataset<Row> rows;
		rows = SparkConnectionMgr.getRowsFromKeyValue("myredis",keypattern);
		// Transform to POJO. Based on Row containing (String key, String value)
		finalKeypattern = keypatternAllVariables;
		Dataset<Product> res = rows.map((MapFunction<Row, Product>) r -> {
					Product product_res = new Product();
					Integer groupindex = null;
					String regex = null;
					String value = null;
					Pattern p, pattern = null;
					Matcher m, match = null;
					String key="";
					boolean matches = false;
					// attribute [Product.Id]
					// Attribute mapped in a key.
					key = r.getAs("key");
					regex = finalKeypattern.replaceAll("\\*","(.*)");
					groupindex = fieldsListInKey.indexOf("prodID")+1;
					if(groupindex==null) {
						logger.warn("Attribute of 'Product' mapped physical field 'prodID' found in key but can't get index in build keypattern '{}'.", finalKeypattern);
					}
					p = Pattern.compile(regex);
					m = p.matcher(key);
					matches = m.find();
					String id = null;
					if(matches) {
						id = m.group(groupindex.intValue());
					} else {
						logger.warn("Cannot retrieve value for Productid attribute stored in db myredis. Probably due to an ambiguous regex.");
						product_res.addLogEvent("Cannot retrieve value for Product.id attribute stored in db myredis. Probably due to an ambiguous regex.");
					}
					product_res.setId(id == null ? null : id);
					// attribute [Product.Photo]
					// Attribute mapped in value part.
					value = r.getAs("value");
					String photo = value == null ? null : value;
					product_res.setPhoto(photo);
					
						
					return product_res;
				}, Encoders.bean(Product.class));
		if(refilterFlag.booleanValue())
			res = res.filter((FilterFunction<Product>) r -> condition == null || condition.evaluate(r));
		res=res.dropDuplicates();
		return res;
		
	}
	
	public static Pair<String, List<String>> getSQLWhereClauseInProductCatalogTableFromMyproductdb(Condition<ProductAttribute> condition, MutableBoolean refilterFlag) {
		return getSQLWhereClauseInProductCatalogTableFromMyproductdbWithTableAlias(condition, refilterFlag, "");
	}
	
	public static Pair<String, List<String>> getSQLWhereClauseInProductCatalogTableFromMyproductdbWithTableAlias(Condition<ProductAttribute> condition, MutableBoolean refilterFlag, String tableAlias) {
		String where = null;	
		List<String> preparedValues = new java.util.ArrayList<String>();
		if(condition != null) {
			
			if(condition instanceof SimpleCondition) {
				ProductAttribute attr = ((SimpleCondition<ProductAttribute>) condition).getAttribute();
				Operator op = ((SimpleCondition<ProductAttribute>) condition).getOperator();
				Object value = ((SimpleCondition<ProductAttribute>) condition).getValue();
				if(value != null) {
					boolean isConditionAttrEncountered = false;
					if(attr == ProductAttribute.id ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "product_id " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(attr == ProductAttribute.price ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = "@VAR@$";
						Boolean like_op = false;
						if(op == Operator.EQUALS && valueString != null && preparedValue.contains("@OTHERVAR@")) {
							//ex: @@VAR@@' '@@OTHERVAR@@"=> more than one vars in LongField: we shall use regex
							like_op = true;
							sqlOp = "LIKE";
							preparedValue = "@VAR@$";
						} else {
							if(op == Operator.NOT_EQUALS && valueString != null && preparedValue.contains("@OTHERVAR@")) {
								//ex: @@VAR@@' '@@OTHERVAR@@"=> more than one vars in LongField: we shall use regex
								sqlOp = "NOT LIKE";
								like_op = true;
								preparedValue = "@VAR@$";
							}
						}
						if(op == Operator.CONTAINS && valueString != null) {
							like_op = true;
							preparedValue = "@VAR@$";
							preparedValue = preparedValue.replaceAll("@VAR@", "%@VAR@%");
						}
						
						if(like_op)
							valueString = Util.escapeReservedCharSQL(valueString);
						preparedValue = preparedValue.replaceAll("@VAR@", valueString).replaceAll("@OTHERVAR@", "%");
						
						where = tableAlias + "dollarprice " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(attr == ProductAttribute.description ) {
						isConditionAttrEncountered = true;
						String valueString = Util.transformSQLValue(value);
						String sqlOp = op.getSQLOperator();
						String preparedValue = valueString;
						if(op == Operator.CONTAINS && valueString != null) {
							preparedValue = "%" + Util.escapeReservedCharSQL(valueString)  + "%";
						}
						
						where = tableAlias + "description " + sqlOp + " ?";
						preparedValues.add(preparedValue);
					}
					if(!isConditionAttrEncountered) {
						refilterFlag.setValue(true);
						where = "1 = 1";
					}
				}
			}
	
			if(condition instanceof AndCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInProductCatalogTableFromMyproductdb(((AndCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInProductCatalogTableFromMyproductdb(((AndCondition) condition).getRightCondition(), refilterFlag);
				String whereLeft = pairLeft.getKey();
				String whereRight = pairRight.getKey();
				List<String> leftValues = pairLeft.getValue();
				List<String> rightValues = pairRight.getValue();
				if(whereLeft != null || whereRight != null) {
					if(whereLeft == null)
						where = whereRight;
					else
						if(whereRight == null)
							where = whereLeft;
						else
							where = "(" + whereLeft + " AND " + whereRight + ")";
					preparedValues.addAll(leftValues);
					preparedValues.addAll(rightValues);
				}
			}
	
			if(condition instanceof OrCondition) {
				Pair<String, List<String>> pairLeft = getSQLWhereClauseInProductCatalogTableFromMyproductdb(((OrCondition) condition).getLeftCondition(), refilterFlag);
				Pair<String, List<String>> pairRight = getSQLWhereClauseInProductCatalogTableFromMyproductdb(((OrCondition) condition).getRightCondition(), refilterFlag);
				String whereLeft = pairLeft.getKey();
				String whereRight = pairRight.getKey();
				List<String> leftValues = pairLeft.getValue();
				List<String> rightValues = pairRight.getValue();
				if(whereLeft != null || whereRight != null) {
					if(whereLeft == null)
						where = whereRight;
					else
						if(whereRight == null)
							where = whereLeft;
						else
							where = "(" + whereLeft + " OR " + whereRight + ")";
					preparedValues.addAll(leftValues);
					preparedValues.addAll(rightValues);
				}
			}
	
		}
	
		return new ImmutablePair<String, List<String>>(where, preparedValues);
	}
	
	
	public Dataset<Product> getProductListInProductCatalogTableFromMyproductdb(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag){
	
		Pair<String, List<String>> whereClause = ProductServiceImpl.getSQLWhereClauseInProductCatalogTableFromMyproductdb(condition, refilterFlag);
		String where = whereClause.getKey();
		List<String> preparedValues = whereClause.getValue();
		for(String preparedValue : preparedValues) {
			where = where.replaceFirst("\\?", "'" + Util.escapeQuote(preparedValue) + "'");
		}
		
		Dataset<Row> d = dbconnection.SparkConnectionMgr.getDataset("myproductdb", "ProductCatalogTable");
		if(where != null) {
			d = d.where(where);
		}
	
		Dataset<Product> res = d.map((MapFunction<Row, Product>) r -> {
					Product product_res = new Product();
					Integer groupIndex = null;
					String regex = null;
					String value = null;
					Pattern p = null;
					Matcher m = null;
					boolean matches = false;
					
					// attribute [Product.Id]
					String id = Util.getStringValue(r.getAs("product_id"));
					product_res.setId(id);
					
					// attribute [Product.Price]
					regex = "(.*)(\\$)";
					groupIndex = 1;
					if(groupIndex == null) {
						logger.warn("Cannot retrieve value for Productprice attribute stored in db myproductdb. Probably due to an ambiguous regex.");
						product_res.addLogEvent("Cannot retrieve value for Productprice attribute stored in db myproductdb. Probably due to an ambiguous regex.");
					}
					value = r.getAs("dollarprice");
					p = Pattern.compile(regex);
					m = p.matcher(value);
					matches = m.find();
					String price = null;
					if(matches) {
						price = m.group(groupIndex.intValue());
					} else {
						logger.warn("Cannot retrieve value for Productprice attribute stored in db myproductdb. Probably due to an ambiguous regex.");
						product_res.addLogEvent("Cannot retrieve value for Product.price attribute stored in db myproductdb. Probably due to an ambiguous regex.");
					}
					product_res.setPrice(price == null ? null : Integer.parseInt(price));
					
					// attribute [Product.Description]
					String description = Util.getStringValue(r.getAs("description"));
					product_res.setDescription(description);
	
	
	
					return product_res;
				}, Encoders.bean(Product.class));
	
	
		return res;
		
	}
	
	
	// TODO get based on id(s). Ex:public Client getClientById(Long id)
	
	public Dataset<Product> getProductListById(String id) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.id, conditions.Operator.EQUALS, id));
	}
	
	public Dataset<Product> getProductListByName(String Name) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.Name, conditions.Operator.EQUALS, Name));
	}
	
	public Dataset<Product> getProductListByPhoto(String photo) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.photo, conditions.Operator.EQUALS, photo));
	}
	
	public Dataset<Product> getProductListByPrice(Integer price) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.price, conditions.Operator.EQUALS, price));
	}
	
	public Dataset<Product> getProductListByDescription(String description) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.description, conditions.Operator.EQUALS, description));
	}
	
	public Dataset<Product> getProductListByCategory(String category) {
		return getProductList(conditions.Condition.simple(conditions.ProductAttribute.category, conditions.Operator.EQUALS, category));
	}
	
	
	
	
	
	public boolean insertProduct(Product product){
		// Insert into all mapped standalone AbstractPhysicalStructure 
		boolean inserted = false;
			inserted = insertProductInCategoryCollectionFromMymongo2(product) || inserted ;
			inserted = insertProductInKVProdPriceFromMyredis(product) || inserted ;
			inserted = insertProductInKVProdPhotosFromMyredis(product) || inserted ;
			inserted = insertProductInProductCatalogTableFromMyproductdb(product) || inserted ;
		return inserted;
	}
	
	public boolean insertProductInCategoryCollectionFromMymongo2(Product product)	{
		Condition<ProductAttribute> conditionID;
		String idvalue="";
		conditionID = Condition.simple(ProductAttribute.id, Operator.EQUALS, product.getId());
		idvalue+=product.getId();
		boolean entityExists=false;
		entityExists = !getProductListInCategoryCollectionFromMymongo2(conditionID,new MutableBoolean(false)).isEmpty();
				
		if(!entityExists){
		List<Row> listRows=new ArrayList<Row>();
		List<Object> valuescategoryCollection_1 = new ArrayList<>();
		List<StructField> listOfStructFieldcategoryCollection_1 = new ArrayList<StructField>();
		if(!listOfStructFieldcategoryCollection_1.contains(DataTypes.createStructField("categoryname",DataTypes.StringType, true)))
			listOfStructFieldcategoryCollection_1.add(DataTypes.createStructField("categoryname",DataTypes.StringType, true));
		valuescategoryCollection_1.add(product.getCategory());
			Object row_value_products_2 = null;
		// Embedded structure products
						List<Object> valuesproducts_2 = new ArrayList<>();
						List<StructField> listOfStructFieldproducts_2 = new ArrayList<StructField>();
						if(!listOfStructFieldproducts_2.contains(DataTypes.createStructField("id",DataTypes.StringType, true)))
							listOfStructFieldproducts_2.add(DataTypes.createStructField("id",DataTypes.StringType, true));
						valuesproducts_2.add(product.getId());
						if(!listOfStructFieldproducts_2.contains(DataTypes.createStructField("name",DataTypes.StringType, true)))
							listOfStructFieldproducts_2.add(DataTypes.createStructField("name",DataTypes.StringType, true));
						valuesproducts_2.add(product.getName());
						
				StructType structType_categoryCollection_1 = DataTypes.createStructType(listOfStructFieldproducts_2);
				ArrayType arrayproducts_1 = DataTypes.createArrayType(structType_categoryCollection_1);
				listOfStructFieldcategoryCollection_1.add(DataTypes.createStructField("products",arrayproducts_1,true));
				row_value_products_2 = Arrays.asList(RowFactory.create(valuesproducts_2.toArray()));
				valuescategoryCollection_1.add(row_value_products_2);
		
		StructType struct = DataTypes.createStructType(listOfStructFieldcategoryCollection_1);
		listRows.add(RowFactory.create(valuescategoryCollection_1.toArray()));
		SparkConnectionMgr.writeDataset(listRows, struct, "mongo", "categoryCollection", "mymongo2");
			logger.info("Inserted [Product] entity ID [{}] in [CategoryCollection] in database [Mymongo2]", idvalue);
		}
		else
			logger.warn("[Product] entity ID [{}] already present in [CategoryCollection] in database [Mymongo2]", idvalue);
		return !entityExists;
	} 
	public boolean insertProductInKVProdPriceFromMyredis(Product product)	{
		Condition<ProductAttribute> conditionID;
		String idvalue="";
		conditionID = Condition.simple(ProductAttribute.id, Operator.EQUALS, product.getId());
		idvalue+=product.getId();
		boolean entityExists=false;
		entityExists = !getProductListInKVProdPriceFromMyredis(conditionID,new MutableBoolean(false)).isEmpty();
				
		if(!entityExists){
			String key="";
			boolean toAdd = false;
			key += "PRODUCT:";
			key += product.getId();
			key += ":PRICE";
			
			String value="";
			if(product.getPrice()!=null){
				toAdd = true;
				value += product.getPrice();
			}
			//No addition of key value pair when the value is null.
			if(toAdd)
				SparkConnectionMgr.writeKeyValue(key,value,"myredis");
	
			logger.info("Inserted [Product] entity ID [{}] in [KVProdPrice] in database [Myredis]", idvalue);
		}
		else
			logger.warn("[Product] entity ID [{}] already present in [KVProdPrice] in database [Myredis]", idvalue);
		return !entityExists;
	} 
	public boolean insertProductInKVProdPhotosFromMyredis(Product product)	{
		Condition<ProductAttribute> conditionID;
		String idvalue="";
		conditionID = Condition.simple(ProductAttribute.id, Operator.EQUALS, product.getId());
		idvalue+=product.getId();
		boolean entityExists=false;
		entityExists = !getProductListInKVProdPhotosFromMyredis(conditionID,new MutableBoolean(false)).isEmpty();
				
		if(!entityExists){
			String key="";
			boolean toAdd = false;
			key += "PRODUCT:";
			key += product.getId();
			key += ":PHOTO";
			
			String value="";
			if(product.getPhoto()!=null){
				toAdd = true;
				value += product.getPhoto();
			}
			//No addition of key value pair when the value is null.
			if(toAdd)
				SparkConnectionMgr.writeKeyValue(key,value,"myredis");
	
			logger.info("Inserted [Product] entity ID [{}] in [KVProdPhotos] in database [Myredis]", idvalue);
		}
		else
			logger.warn("[Product] entity ID [{}] already present in [KVProdPhotos] in database [Myredis]", idvalue);
		return !entityExists;
	} 
	public boolean insertProductInProductCatalogTableFromMyproductdb(Product product)	{
		Condition<ProductAttribute> conditionID;
		String idvalue="";
		conditionID = Condition.simple(ProductAttribute.id, Operator.EQUALS, product.getId());
		idvalue+=product.getId();
		boolean entityExists=false;
		entityExists = !getProductListInProductCatalogTableFromMyproductdb(conditionID,new MutableBoolean(false)).isEmpty();
				
		if(!entityExists){
		List<Row> listRows=new ArrayList<Row>();
		List<Object> valuesProductCatalogTable_1 = new ArrayList<>();
		List<StructField> listOfStructFieldProductCatalogTable_1 = new ArrayList<StructField>();
		if(!listOfStructFieldProductCatalogTable_1.contains(DataTypes.createStructField("product_id",DataTypes.StringType, true)))
			listOfStructFieldProductCatalogTable_1.add(DataTypes.createStructField("product_id",DataTypes.StringType, true));
		valuesProductCatalogTable_1.add(product.getId());
		if(!listOfStructFieldProductCatalogTable_1.contains(DataTypes.createStructField("description",DataTypes.StringType, true)))
			listOfStructFieldProductCatalogTable_1.add(DataTypes.createStructField("description",DataTypes.StringType, true));
		valuesProductCatalogTable_1.add(product.getDescription());
		String value_ProductCatalogTable_dollarprice_1 = "";
		value_ProductCatalogTable_dollarprice_1 += product.getPrice();
		value_ProductCatalogTable_dollarprice_1 += "$";
		if(!listOfStructFieldProductCatalogTable_1.contains(DataTypes.createStructField("dollarprice",DataTypes.StringType, true)))
			listOfStructFieldProductCatalogTable_1.add(DataTypes.createStructField("dollarprice",DataTypes.StringType, true));
		valuesProductCatalogTable_1.add(value_ProductCatalogTable_dollarprice_1);
		
		StructType structType = DataTypes.createStructType(listOfStructFieldProductCatalogTable_1);
		listRows.add(RowFactory.create(valuesProductCatalogTable_1.toArray()));
		SparkConnectionMgr.writeDataset(listRows, structType, "jdbc", "ProductCatalogTable", "myproductdb");
			logger.info("Inserted [Product] entity ID [{}] in [ProductCatalogTable] in database [Myproductdb]", idvalue);
		}
		else
			logger.warn("[Product] entity ID [{}] already present in [ProductCatalogTable] in database [Myproductdb]", idvalue);
		return !entityExists;
	} 
	
	
	public void updateProductList(conditions.Condition<conditions.ProductAttribute> condition, conditions.SetClause<conditions.ProductAttribute> set){
		//TODO
	}
	
	public void updateProduct(pojo.Product product) {
		//TODO using the id
		return;
	}
	
	
	public void deleteProductList(conditions.Condition<conditions.ProductAttribute> condition){
		//TODO
	}
	
	public void deleteProduct(pojo.Product product) {
		//TODO using the id
		return;
	}
	
}
