conceptual schema cs {
	entity type Product{
		id:string,
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
		id : string,
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
	relational schema myRelSchema : mydb {
		table ProductCatalogTable {
			columns {
				product_id,
				europrice : [price]"€",
				description,
				categoryname
			}
		}
		
		table ReviewTable {
			columns {
				review_id,
				rating,
				content,
				product_ref
			}
			
			references{
				reviewed_product : product_ref -> ProductCatalogTable.product_id
			}
		}
	}
}
	
mapping rules{
	cs.Product(id,description,price) -> myRelSchema.ProductCatalogTable(product_id,description,price),
	cs.Review(content,id,rating) -> myRelSchema.ReviewTable(content,review_id,rating),
	cs.productReview.review -> myRelSchema.ReviewTable.reviewed_product
}

databases {
	
	mariadb mydb {
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
