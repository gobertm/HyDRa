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
		identifier{
			id
		}
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
		
		// Standalone struct Client hash		
		kvpairs KVClient {
			key : "CLIENT:"[clientID],
			value : hash { 
				name : [firstname]"_"[lastname],
				streetnumber : [streetnbr], 
				street
			}
		}
		
		// Standalone struct Product string key/value
		kvpairs kvProductName {
			key : "PRODUCT:"[idprod]":NAME",
			value : name
		}
		// Standalone struct Product string key/value
		kvpairs kvProdPrice {
			key : "PRODUCT:"[idprod]":PRICE",
			value : [price]"$"
		}
		
		kvpairs kvReview {
			// Many to one
			key: "REVIEW:"[reviewid],
			value: hash{
				content,
				stars : [rate]"*",
				posted_by
				}
				
			references {
			post_review : kvReview.posted_by -> KVClient.clientID
			}
		}
		
		// One to many
		kvpairs kvProductList {
			key : "PRODUCT:"[prodid]":REVIEWS",
			value : list {
				idreview
			}
			references{
				reviews : idreview -> kvReview.reviewid 
			}
		}
	}
}

mapping rules{
	cs.Review(id) -> KVSchema.kvReview(reviewid),
	cs.Review(content,rating) -> KVSchema.kvReview(content,rate),
	cs.reviewClient.review -> KVSchema.kvReview.post_review,
	cs.Client(id) -> KVSchema.KVClient(clientID),
	cs.Client(firstname,lastname,street,number) -> KVSchema.KVClient(firstname,lastname,street,streetnbr),
	cs.productReview.product -> KVSchema.kvProductList.reviews,
	cs.Product(id) -> KVSchema.kvProductList(prodid),
	cs.Product(id,name) -> KVSchema.kvProductName(idprod,name),
	cs.Product(id,price) -> KVSchema.kvProdPrice(idprod,price)
}

databases {
	
	redis myredis{
		host:"localhost"
		port:6379
	}
	
}