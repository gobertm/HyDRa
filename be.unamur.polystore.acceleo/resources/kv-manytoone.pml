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
			value: hash{
				content,
				stars : [rate]"*",
				posted_by
				}
				
			references {
			post_review : kvProdReview.posted_by -> KVClient.clientID
			}
		}
		
		kvpairs KVClient {
			key : "CLIENT:"[clientID],
			value : hash { 
				name : [firstname]"_"[lastname],
				streetnumber : [streetnbr], 
				street
			}
		}
		
//		kvpairs kvReviews {
//			key: "REVIEW:"[reviewid]":content",
//			value : content
//		}
	}
}

mapping rules{
	cs.Product(id) -> KVSchema.kvProdReview(prodid),
	cs.Review(id) -> KVSchema.kvProdReview(reviewid),
	cs.Review(content,rating) -> KVSchema.kvProdReview(content,rate),
//	cs.Review(id,content) -> KVSchema.kvReviews(reviewid,content),
	cs.reviewClient.review -> KVSchema.kvProdReview.post_review,
	cs.Client(id) -> KVSchema.KVClient(clientID),
	cs.Client(firstname,lastname,street,number) -> KVSchema.KVClient(firstname,lastname,street,streetnbr)
}

databases {
	
	redis myredis{
		host:"localhost"
		port:6379
	}
	
}