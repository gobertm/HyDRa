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
		id: string,
		rating : int,
		content : string
	}
	
	
	relationship type productReview{
	reviews[1]: Review,
	product[0-N] : Product
	}
	
}

physical schemas {
	key value schema KVSchema : myredis {
		kvpairs kvProdReview {
			key: "PRODUCT:"[prodid]":REVIEW:"[reviewid],
			value: reviews hash{
				content,
				stars : [rate]"*"
				}
		}
	}
}

mapping rules{
	cs.Product(id) -> KVSchema.kvProdReview(prodid),
	cs.Review(id) -> KVSchema.kvProdReview(reviewid),
	cs.Review(content,rating) -> KVSchema.kvProdReview.reviews(content,rate)
}

databases {
	
	redis myredis{
		host:"localhost"
		port:6363
	}
	
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