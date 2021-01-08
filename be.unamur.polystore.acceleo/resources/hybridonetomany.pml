conceptual schema cs {
	entity type Product{
		id:int,
		name:string,
		price:float,
		description:string,
		cat_name : string,
		cat_description: string

		identifier{
			id
		}
	} 
	
	entity type Review {
		id : int,
		rating : int,
		content : string
		
		identifier {
			id
		}
	}
	
	
	relationship type productReview{
	review[1]: Review,
	reviewed_product[0-N] : Product
	}
	
}
physical schemas {
	relational schema myRelSchema : mysql {
		table ProductCatalogTable {
			columns {
				product_id,
				europrice : [price]"€",
				description
			}
		}
		
		table ReviewTable {
			columns {
				review_id,
				rating,
				content,
				product_ref,
				product_ref_mongo
			}
			
			references{
				reviewed_product : product_ref -> ProductCatalogTable.product_id
				mongo_reviewed_product : product_ref_mongo -> myDocSchema.productCollection.doc_prod_id
			}
		}
	}
	
	document schema myDocSchema : mymongo {
		collection productCollection {
			fields {
				doc_prod_id,
				prodname,
				description,
				category [1] {
					categoryname,
					categorydescription
				}
			}
		}
	}
}
	
mapping rules{
	cs.Product(id,description,price) -> myRelSchema.ProductCatalogTable(product_id,description,price),
	cs.Review(content,id,rating) -> myRelSchema.ReviewTable(content,review_id,rating),
	cs.productReview.review -> myRelSchema.ReviewTable.reviewed_product,
	cs.Product(id,name,description) -> myDocSchema.productCollection(doc_prod_id,prodname,description),
	cs.Product(cat_name, cat_description) -> myDocSchema.productCollection.category(categoryname,categorydescription),
	cs.productReview.review -> myRelSchema.ReviewTable.mongo_reviewed_product
}

databases {
	
	mariadb mysql {
		host: "localhost"
		port: 3307
		dbname : "mydb"
		password : "password"
		login : "root"
	}
	
	mongodb mymongo {
		host : "localhost"
		port:27000
	}
	
	mongodb mymongo2 {
		host:"locahost"
		port: 27100
			}
}
