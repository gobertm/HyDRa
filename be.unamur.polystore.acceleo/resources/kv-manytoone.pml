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
	
	entity type Client {
	id : string,
	firstname : string,
	lastname : string,
	street : string,
	number : int
		identifier {
			id
		}
	}
	
	relationship type productReview{
	reviews[1]: Review,
	product[0-N] : Product
	}
	
	relationship type reviewClient {
		poster[0-N] : Client,
		review[1] : Review
	}
	
}

physical schemas {
	key value schema KVSchema : myredis {
		kvpairs kvProdReview {
			key: "PRODUCT:"[prodid]":REVIEW:"[reviewid],
			value: reviews hash{
				content,
				stars : [rate]"*",
				posted_by
				}
				
			references {
			post_review : kvProdReview.reviews.posted_by -> KVClient.clientID
			}
		}
		
		kvpairs KVClient {
			key : "CLIENT:"[clientID],
			value : attr hash { 
				name : [firstname]"_"[lastname],
				streetnumber : [streetnbr], 
				street
			}
		}
	}
}

mapping rules{
	cs.Product(id) -> KVSchema.kvProdReview(prodid),
	cs.Review(id) -> KVSchema.kvProdReview(reviewid),
	cs.Review(content,rating) -> KVSchema.kvProdReview.reviews(content,rate),
	cs.reviewClient.review -> KVSchema.kvProdReview.post_review,
	cs.Client(id) -> KVSchema.KVClient(clientID),
	cs.Client(firstname,lastname,street,number) -> KVSchema.KVClient.attr(firstname,lastname,street,streetnbr)
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