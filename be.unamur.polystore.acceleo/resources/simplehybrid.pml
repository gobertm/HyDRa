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
		rating : int,
		content : string
	}
	
	
	relationship type productReview{
	reviews[1]: Review,
	product[0-N] : Product
	}
	
}
physical schemas {
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
	
	document schema categorySchema : mymongo2 {
			collection categoryCollection {
				fields {
					categoryname,
					products[0-N]{
						id
					}
				}
			}
		
	}
	
	relational schema myRelSchema : mydb {
		table ProductCatalogTable {
			columns {
				product_id,
				europrice : [price]"€",
				description,
				categoryname
			}
		}
	}
}
	
mapping rules{
	cs.Product(id,description,price,name) -> myDocSchema.productCollection(product_ref,productDescription,price,name),
	cs.Product(id) -> categorySchema.categoryCollection.products(id),
	cs.Product(cat_name) -> categorySchema.categoryCollection(categoryname),
	cs.Review(content,rating,rating) ->myDocSchema.productCollection.reviews(content,rate,rate2),
	cs.Product(id,price,description,cat_name) -> myRelSchema.ProductCatalogTable(product_id,price,description,categoryname)
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
