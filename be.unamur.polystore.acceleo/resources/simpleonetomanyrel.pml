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
	relational schema myRelSchema : mysqlite {
		table ProductCatalogTable {
			columns {
				product_id,
				europrice : [price]"�",
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
	
	sqlite mysqlite {
		host: "sqlite.unamur.be"
		port: 8090
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
