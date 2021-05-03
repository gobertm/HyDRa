conceptual schema cs {
	entity type Product{
		id:string,
		name:string,
		price:float,
		description:string,
		cat_name : string,
		cat_description: string,
		photo : blob

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
		id : int,
		firstname : string,
		lastname : string,
		street : string,
		number : int
		
		identifier {
			id
		}
	}
	
	entity type CreditCard{
		id : string,
		number : int,
		expirymonth : date // add format 
	}
	
	entity type Order{
		id : string,
		orderDate : date
	}
	
	relationship type productReview{
		reviews[1]: Review,
		product[0-N] : Product
	}
	
	relationship type reviewClient {
		poster[0-N] : Client,
		review[1] : Review
	}
	
	relationship type orderClient {
		order[1] : Order,
		customer[0-N] : Client
	}
	
	relationship type orderCreditCard {
		order [0-1] : Order,
		payment_card [0-N] : CreditCard
	}

}
physical schemas {
	document schema myDocSchema : mymongo {
		collection productCollection{
			fields { 
				id,
				Name,
				Description,
				price,
				review[0-N]{
					numberofstars:[rate],
					ratingstring :[rate2]"*",
					content,
					posted_by
				}
			}
			references {
				poster : review.posted_by -> KVSchema.KVClient.clientID
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
		table CreditCard {
			columns {
				id,
				number,
				expiryDate
			}
		} 
		
		table Order {
			columns{ 
				id,
				dateOrder,
				cust_id,
				card_id
			}	
			references {
				 bought_by : cust_id -> KVSchema.KVClient.clientID
				 paid_with : card_id -> CreditCard.id
			}
		}
		
		table ArchiveOrder {
			columns{ 
				id,
				dateOrder,
				cust_id,
				card_id
			}	
			references {
				 bought_by : cust_id -> KVSchema.KVClient.clientID
				 paid_with : card_id -> CreditCard.id
			}
		}
	}
	
	graph schema myGraphSchema {
		Node Product {
			product_id,
			Name,
			Description
		}
		
		Node Order {
			orderid
		}
		
		Edge CONTAINS {
			Product -> Order,
			quantity  // How to map this one?
		}
	}
	
	key value schema KVSchema : myredis {
	
		kvpairs KVClient {
			key : "CLIENT:"[clientID],
			value : attr hash { 
				name : [firstname]"_"[lastname],
				streetnumber : [streetnbr], 
				street
			}
		}
		
		kvpairs kvProdPhotos {
			key:"PRODUCT:"[prodid],
			value : photo
		}
	}
	
	column schema colSchema {
		
		table Client {
			rowkey{
				clientnumber
			}
			
			columnfamilies {
				personnal {
					name:[first]"_"[last]
					}
				address{
					street,
					number,
					zipcode
				}
			}
		}
	}
}

mapping rules { 
	  cs.Product(id, name, description, price) -> myDocSchema.productCollection(id,Name,Description, price),
	  cs.Product(cat_name) -> categorySchema.categoryCollection(categoryname),
	  cs.Product(id) -> categorySchema.categoryCollection.products(id),
	  cs.Product(id,name,description) -> myGraphSchema.Product(product_id,Name,Description),
	  cs.Product(id,photo) -> KVSchema.kvProdPhotos(prodid,photo),
	  cs.Review(content,rating) -> myDocSchema.productCollection.review(content,rate),
	  cs.Review(rating) -> myDocSchema.productCollection.review(rate2),
	  cs.reviewClient.review -> myDocSchema.productCollection.poster,
	  cs.productReview.reviews -> myDocSchema.productCollection.review(),
	  cs.CreditCard(id,number, expirymonth) -> myRelSchema.CreditCard(id,number,expiryDate),
	  cs.orderCreditCard.order -> myRelSchema.Order.paid_with,
	  cs.Order(id, orderDate) -> myRelSchema.Order(id,dateOrder),
	  cs.Order(id, orderDate) -(orderDate < "2019")-> myRelSchema.ArchiveOrder(id,dateOrder),
	  cs.Order(id) -> myGraphSchema.Order(orderid),
	  cs.Client(lastname,firstname) -> colSchema.Client(first,last),
	  cs.Client(id) -> KVSchema.KVClient(clientID),
	  cs.Client(firstname, lastname, street, number) -> KVSchema.KVClient.attr(firstname, lastname, street, streetnbr)
	  
	}
	
databases{
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
	