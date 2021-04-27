package dao.services;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pojo.Product;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.commons.lang.mutable.MutableBoolean;
import conditions.Condition;
import conditions.Operator;

public abstract class ProductService {
	static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProductService.class);
	


	public static enum ROLE_NAME {
		
	}
	private static java.util.Map<ROLE_NAME, loading.Loading> defaultLoadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	static {
	}
	
	private java.util.Map<ROLE_NAME, loading.Loading> loadingParameters = new java.util.HashMap<ROLE_NAME, loading.Loading>();
	
	public ProductService() {
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
			loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public ProductService(java.util.Map<ROLE_NAME, loading.Loading> loadingParams) {
		this();
		if(loadingParams != null)
			for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: loadingParams.entrySet())
				loadingParameters.put(entry.getKey(), entry.getValue());
	}
	
	public static java.util.Map<ROLE_NAME, loading.Loading> getDefaultLoadingParameters() {
		java.util.Map<ROLE_NAME, loading.Loading> res = new java.util.HashMap<ROLE_NAME, loading.Loading>();
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
				res.put(entry.getKey(), entry.getValue());
		return res;
	}
	
	public static void setAllDefaultLoadingParameters(loading.Loading loading) {
		java.util.Map<ROLE_NAME, loading.Loading> newParams = new java.util.HashMap<ROLE_NAME, loading.Loading>();
		for(java.util.Map.Entry<ROLE_NAME, loading.Loading> entry: defaultLoadingParameters.entrySet())
				newParams.put(entry.getKey(), entry.getValue());
		defaultLoadingParameters = newParams;
	}
	
	public java.util.Map<ROLE_NAME, loading.Loading> getLoadingParameters() {
		return this.loadingParameters;
	}
	
	public void setLoadingParameters(java.util.Map<ROLE_NAME, loading.Loading> newParams) {
		this.loadingParameters = newParams;
	}
	
	public void updateLoadingParameter(ROLE_NAME role, loading.Loading l) {
		this.loadingParameters.put(role, l);
	}
	
	
	public Dataset<Product> getProductList(conditions.Condition<conditions.ProductAttribute> condition){
		MutableBoolean refilterFlag = new MutableBoolean(false);
		List<Dataset<Product>> datasets = new ArrayList<Dataset<Product>>();
		Dataset<Product> d = null;
		d = getProductListInProductCatalogTableFromMyproductdb(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		d = getProductListInKVProdPriceFromMyredis(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		d = getProductListInKVProdPhotosFromMyredis(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		d = getProductListInCategoryCollectionFromMymongo2(condition, refilterFlag);
		if(d != null)
			datasets.add(d);
		
		if(datasets.size() == 0)
			return null;
	
		d = datasets.get(0);
		if(datasets.size() > 1) {
	
		
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasets.get(1)
								.withColumnRenamed("name", "name_1")
								.withColumnRenamed("photo", "photo_1")
								.withColumnRenamed("price", "price_1")
								.withColumnRenamed("description", "description_1")
								.withColumnRenamed("category", "category_1")
							, seq, "fullouter");
			for(int i = 2; i < datasets.size(); i++) {
				res = res.join(datasets.get(i)
								.withColumnRenamed("name", "name_" + i)
								.withColumnRenamed("photo", "photo_" + i)
								.withColumnRenamed("price", "price_" + i)
								.withColumnRenamed("description", "description_" + i)
								.withColumnRenamed("category", "category_" + i)
							, seq, "fullouter");
			} 
			d = res.map((MapFunction<Row, Product>) r -> {
					Product product_res = new Product();
					
					// attribute 'Product.id'
					String firstNotNull_id = r.getAs("id");
					product_res.setId(firstNotNull_id);
					
					// attribute 'Product.name'
					String firstNotNull_Name = r.getAs("name");
					for (int i = 1; i < datasets.size(); i++) {
						String name2 = r.getAs("name_" + i);
						if (firstNotNull_Name != null && name2 != null && !firstNotNull_Name.equals(name2)) {
							product_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Product.name': " + firstNotNull_Name + " and " + name2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Product.name' ==> " + firstNotNull_Name + " and " + name2);
						}
						if (firstNotNull_Name == null && name2 != null) {
							firstNotNull_Name = name2;
						}
					}
					product_res.setName(firstNotNull_Name);
					
					// attribute 'Product.photo'
					String firstNotNull_photo = r.getAs("photo");
					for (int i = 1; i < datasets.size(); i++) {
						String photo2 = r.getAs("photo_" + i);
						if (firstNotNull_photo != null && photo2 != null && !firstNotNull_photo.equals(photo2)) {
							product_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Product.photo': " + firstNotNull_photo + " and " + photo2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Product.photo' ==> " + firstNotNull_photo + " and " + photo2);
						}
						if (firstNotNull_photo == null && photo2 != null) {
							firstNotNull_photo = photo2;
						}
					}
					product_res.setPhoto(firstNotNull_photo);
					
					// attribute 'Product.price'
					Integer firstNotNull_price = r.getAs("price");
					for (int i = 1; i < datasets.size(); i++) {
						Integer price2 = r.getAs("price_" + i);
						if (firstNotNull_price != null && price2 != null && !firstNotNull_price.equals(price2)) {
							product_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Product.price': " + firstNotNull_price + " and " + price2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Product.price' ==> " + firstNotNull_price + " and " + price2);
						}
						if (firstNotNull_price == null && price2 != null) {
							firstNotNull_price = price2;
						}
					}
					product_res.setPrice(firstNotNull_price);
					
					// attribute 'Product.description'
					String firstNotNull_description = r.getAs("description");
					for (int i = 1; i < datasets.size(); i++) {
						String description2 = r.getAs("description_" + i);
						if (firstNotNull_description != null && description2 != null && !firstNotNull_description.equals(description2)) {
							product_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Product.description': " + firstNotNull_description + " and " + description2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Product.description' ==> " + firstNotNull_description + " and " + description2);
						}
						if (firstNotNull_description == null && description2 != null) {
							firstNotNull_description = description2;
						}
					}
					product_res.setDescription(firstNotNull_description);
					
					// attribute 'Product.category'
					String firstNotNull_category = r.getAs("category");
					for (int i = 1; i < datasets.size(); i++) {
						String category2 = r.getAs("category_" + i);
						if (firstNotNull_category != null && category2 != null && !firstNotNull_category.equals(category2)) {
							product_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Product.category': " + firstNotNull_category + " and " + category2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Product.category' ==> " + firstNotNull_category + " and " + category2);
						}
						if (firstNotNull_category == null && category2 != null) {
							firstNotNull_category = category2;
						}
					}
					product_res.setCategory(firstNotNull_category);
					return product_res;
				}, Encoders.bean(Product.class));
		}
		if(refilterFlag.booleanValue())
			d = d.filter((FilterFunction<Product>) r -> condition == null || condition.evaluate(r));
		d=d.dropDuplicates();
		return d;
	}
	
	
	
	public abstract Dataset<Product> getProductListInProductCatalogTableFromMyproductdb(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	public abstract Dataset<Product> getProductListInKVProdPriceFromMyredis(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	public abstract Dataset<Product> getProductListInKVProdPhotosFromMyredis(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag);
	
	
	
	
	public abstract Dataset<Product> getProductListInCategoryCollectionFromMymongo2(conditions.Condition<conditions.ProductAttribute> condition, MutableBoolean refilterFlag);
	
	
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
	
	
	
	protected static Dataset<Product> fullOuterJoinsProduct(List<Dataset<Product>> datasetsPOJO) {
		return fullOuterJoinsProduct(datasetsPOJO, "fullouter");
	}
	
	protected static Dataset<Product> fullLeftOuterJoinsProduct(List<Dataset<Product>> datasetsPOJO) {
		return fullOuterJoinsProduct(datasetsPOJO, "leftouter");
	}
	
	private static Dataset<Product> fullOuterJoinsProduct(List<Dataset<Product>> datasetsPOJO, String joinMode) {
		if(datasetsPOJO.size() == 0)
				return null;
		if(datasetsPOJO.size() == 1)
			return datasetsPOJO.get(0);
		Dataset<Product> d = datasetsPOJO.get(0);
			List<String> idFields = new ArrayList<String>();
			idFields.add("id");
			scala.collection.Seq<String> seq = scala.collection.JavaConverters.asScalaIteratorConverter(idFields.iterator()).asScala().toSeq();
			Dataset<Row> res = d.join(datasetsPOJO.get(1)
								.withColumnRenamed("name", "name_1")
								.withColumnRenamed("photo", "photo_1")
								.withColumnRenamed("price", "price_1")
								.withColumnRenamed("description", "description_1")
								.withColumnRenamed("category", "category_1")
							, seq, joinMode);
			for(int i = 2; i < datasetsPOJO.size(); i++) {
				res = res.join(datasetsPOJO.get(i)
								.withColumnRenamed("name", "name_" + i)
								.withColumnRenamed("photo", "photo_" + i)
								.withColumnRenamed("price", "price_" + i)
								.withColumnRenamed("description", "description_" + i)
								.withColumnRenamed("category", "category_" + i)
							, seq, joinMode);
			} 
			d = res.map((MapFunction<Row, Product>) r -> {
					Product product_res = new Product();
					
					// attribute 'Product.id'
					String firstNotNull_id = r.getAs("id");
					product_res.setId(firstNotNull_id);
					
					// attribute 'Product.name'
					String firstNotNull_Name = r.getAs("name");
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String name2 = r.getAs("name_" + i);
						if (firstNotNull_Name != null && name2 != null && !firstNotNull_Name.equals(name2)) {
							product_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Product.name': " + firstNotNull_Name + " and " + name2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Product.name' ==> " + firstNotNull_Name + " and " + name2);
						}
						if (firstNotNull_Name == null && name2 != null) {
							firstNotNull_Name = name2;
						}
					}
					product_res.setName(firstNotNull_Name);
					
					// attribute 'Product.photo'
					String firstNotNull_photo = r.getAs("photo");
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String photo2 = r.getAs("photo_" + i);
						if (firstNotNull_photo != null && photo2 != null && !firstNotNull_photo.equals(photo2)) {
							product_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Product.photo': " + firstNotNull_photo + " and " + photo2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Product.photo' ==> " + firstNotNull_photo + " and " + photo2);
						}
						if (firstNotNull_photo == null && photo2 != null) {
							firstNotNull_photo = photo2;
						}
					}
					product_res.setPhoto(firstNotNull_photo);
					
					// attribute 'Product.price'
					Integer firstNotNull_price = r.getAs("price");
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						Integer price2 = r.getAs("price_" + i);
						if (firstNotNull_price != null && price2 != null && !firstNotNull_price.equals(price2)) {
							product_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Product.price': " + firstNotNull_price + " and " + price2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Product.price' ==> " + firstNotNull_price + " and " + price2);
						}
						if (firstNotNull_price == null && price2 != null) {
							firstNotNull_price = price2;
						}
					}
					product_res.setPrice(firstNotNull_price);
					
					// attribute 'Product.description'
					String firstNotNull_description = r.getAs("description");
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String description2 = r.getAs("description_" + i);
						if (firstNotNull_description != null && description2 != null && !firstNotNull_description.equals(description2)) {
							product_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Product.description': " + firstNotNull_description + " and " + description2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Product.description' ==> " + firstNotNull_description + " and " + description2);
						}
						if (firstNotNull_description == null && description2 != null) {
							firstNotNull_description = description2;
						}
					}
					product_res.setDescription(firstNotNull_description);
					
					// attribute 'Product.category'
					String firstNotNull_category = r.getAs("category");
					for (int i = 1; i < datasetsPOJO.size(); i++) {
						String category2 = r.getAs("category_" + i);
						if (firstNotNull_category != null && category2 != null && !firstNotNull_category.equals(category2)) {
							product_res.addLogEvent("Data consistency problem: duplicate values found for attribute 'Product.category': " + firstNotNull_category + " and " + category2 + "." );
							logger.warn("data consistency problem: duplicate values for attribute : 'Product.category' ==> " + firstNotNull_category + " and " + category2);
						}
						if (firstNotNull_category == null && category2 != null) {
							firstNotNull_category = category2;
						}
					}
					product_res.setCategory(firstNotNull_category);
					return product_res;
				}, Encoders.bean(Product.class));
			return d;
	}
	
	
	
	
	
	public abstract void insertProductAndLinkedItems(Product product);
	public abstract void insertProduct(Product product);
	
	public abstract void insertProductInProductCatalogTableFromMyproductdb(Product product); public abstract void insertProductInKVProdPriceFromMyredis(Product product); public abstract void insertProductInKVProdPhotosFromMyredis(Product product); public abstract void insertProductInCategoryCollectionFromMymongo2(Product product); 
	public abstract void updateProductList(conditions.Condition<conditions.ProductAttribute> condition, conditions.SetClause<conditions.ProductAttribute> set);
	
	public void updateProduct(pojo.Product product) {
		//TODO using the id
		return;
	}
	
	
	public abstract void deleteProductList(conditions.Condition<conditions.ProductAttribute> condition);
	
	public void deleteProduct(pojo.Product product) {
		//TODO using the id
		return;
	}
	
}
