conceptual schema cs {
	entity type Product{
		id:string,
		name:string,
		price:int,
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
	reviewed_product[0-N]: Product
	}
	
}
physical schemas {
		
	relational schema myRelSchema : mydb {
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
                product_ref
			}
            references{
				reviewed_product : product_ref -> docSchema2.categoryCollection.products.id
			}
		}
	}
	
	document schema myDocSchema : mymongo{
		collection productCollection{
			fields { 
				product_ref,
				productDescription,
				price,
				name,
				reviews[0-N]{
					numberofstars:[rate],
					ratingstring :[rate2]"*",
					content
				}
			}
		}
	}
	
	document schema docSchema2 : mymongo2 {
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
	// Product
	cs.Product(id,description,price,name) -> myDocSchema.productCollection(product_ref,productDescription,price,name),
	cs.Product(id) -> docSchema2.categoryCollection.products(id),
	cs.Product(cat_name) -> docSchema2.categoryCollection(categoryname),
	cs.Product(id,price,description) -> myRelSchema.ProductCatalogTable(product_id,price,description),
	// Review
	cs.Review(content,rating,rating) ->myDocSchema.productCollection.reviews(content,rate,rate2),
	cs.Review(content,id,rating) -> myRelSchema.ReviewTable(content,review_id,rating),
    cs.Review(id,rating) -> docSchema2.categoryCollection.products.reviews(review_ref,rating),  // Add this mapping rule to test detection of data insconsistencies in Reviews
	// TA productReview
	cs.productReview.reviewed_product -> docSchema2.categoryCollection.review_of_product,
	cs.productReview.review -> myRelSchema.ReviewTable.reviewed_product
}

databases {
	
	sqlite mydb {
		host: "localhost"
		port: 3307
		login: "root"
		password: "password"
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
