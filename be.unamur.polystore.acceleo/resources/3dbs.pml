conceptual schema conceptualSchema{
		
	entity type Product {
		id : string,
		Name : string,
        photo : string,
        price:int,
        description : string,
        category : string
        identifier {
        	id
        }
    }
}
physical schemas { 
	key value schema KVSchema : myredis{
		kvpairs KVProdPhotos {
			key:"PRODUCT:"[prodID]":PHOTO",
			value:photo
		}    
	}
	
	relational schema myRelSchema : mydb {
		table ProductCatalogTable {
			columns {
				product_id,
				description,
				europrice : [price]"€"
			}
		}
	}
	
	document schema categorySchema : mymongo2 {
			collection categoryCollection {
				fields {
					categoryname,
					categorydesc,
					products[0-N]{
						id,
						name
					}
				}
			}
		
	}	
}

mapping rules{
	conceptualSchema.Product(id,photo) -> KVSchema.KVProdPhotos(prodID,photo),
	conceptualSchema.Product(id,price, description) -> myRelSchema.ProductCatalogTable(product_id,price,description),
	conceptualSchema.Product(category) -> categorySchema.categoryCollection(categoryname),
	//Nested attributes of Product
	conceptualSchema.Product(id, Name) -> categorySchema.categoryCollection.products(id,name)
}

databases {
	redis myredis{
		host:"localhost"
		port:6363
	}
	
	mariadb mydb {
		host: "localhost"
		port: 3307
		dbname : "mydb"
		password : "password"
		login : "root"
	}
	
	mongodb mymongo2 {
		host:"localhost"
		port: 27100
	}
}