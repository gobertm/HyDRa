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
	review_role[1]: Review,
	product_role[0-N] : Product
	}
	
}
physical schemas {
	relational schema myRelSchema : mydb {
			
		table ReviewTable {
			columns {
				review_id,
				rating,
				content,
                product_ref
			}
            references{
				reviewed_product : product_ref -> categorySchema.categoryCollection.products.id
			}
		}
	}
	
	document schema categorySchema : mymongo2 {
			collection categoryCollection {
				fields {
					categoryname,
					products[0-N]{
						id,
                        reviews [0-N]{
                        	review_ref,
                        	rating
                        }
					}
				}
				
				references {
					review_of_product : products.reviews.review_ref -> myRelSchema.ReviewTable.review_id
				}
			}
		
	}
}
	
mapping rules{
	cs.Review(content,id,rating) -> myRelSchema.ReviewTable(content,review_id,rating),
	cs.productReview.product_role -> categorySchema.categoryCollection.review_of_product,
	cs.Product(cat_name) -> categorySchema.categoryCollection(categoryname),
	cs.Product(id) -> categorySchema.categoryCollection.products(id),
    cs.productReview.review_role -> myRelSchema.ReviewTable.reviewed_product
 //   cs.Review(id,rating) -> categorySchema.categoryCollection.products.reviews(review_ref,rating)  // Add this mapping rule to test detection of data insconsistencies in Reviews
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
		host:"localhost"
		port: 27100
			}
}
