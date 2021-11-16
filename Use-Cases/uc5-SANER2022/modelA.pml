//spark
conceptual schema cs {
	entity type Customer {
		id : string,
		firstname : string,
		lastname : string,
		gender : string,
		birthday : date,
		creationDate : date,
		locationip : string,
		browser : string
		identifier{
			id
		}
	}
	
	entity type Product {
		id : string,
		title : string,
		price : float,
		photo : string
		identifier{
			id
		}
	}
	
	entity type Order {
		id : string,
		orderdate : date,
		totalprice : float
		identifier {
			id
		}
	}
	
	entity type Feedback {
		rate : float,
		content : string,
		product : string,
		customer : string
	}
	
	relationship type buys {
		order[1]: Order,
		client[0-N]: Customer
	}
	
	relationship type composed_of {
		orderP[1-N] : Order,
		orderedProducts[0-N] : Product
	}
	
	relationship type write{
		review[0-N] : Feedback
		reviewer[0-N] : Customer
	}
	
	relationship type has_reviews{
		reviews[0-N]: Feedback,
		reviewedProduct[0-N]:Product 
	}
}

physical schemas {
	relational schema relSchema : mysqlbench {
		table customerTable {
			columns {
				id,
				firstName,
				lastName,
				gender,
				birthday,
				creationDate,
				locationIP,
				browserUsed,
				place
			}
		}
		
		table productTable{
			columns{
				asin,
				title,
				price,
				imgUrl
			}
		}
	}
	
	document schema docSchema : mongobench {
		collection ordersCol {
			fields {
				OrderId,
				PersonId,
				OrderDate,
				TotalPrice,
				Orderline[0-N] {
					asin,
					title,
					price
				}
			}
			references {
				customer : PersonId -> relSchema.customerTable.id
			}
		}
	}
	
	key value schema kvSchema : redisbench {
		kvpairs feedback {
			key : [prodid]":"[customerid],
			value : [rating]"&&"[content]
			references {
				product : prodid -> relSchema.productTable.asin
				customer1 : customerid -> relSchema.customerTable.id
			}
		}
	}
}

mapping rules {
	cs.Product(id, title, price,photo) -> relSchema.productTable(asin, title, price, imgUrl),
	cs.Product( id, title, price) -> docSchema.ordersCol.Orderline( asin, title, price),
	cs.Customer(id,firstname,lastname, gender, birthday, creationDate, locationip, browser) -> relSchema.customerTable(id, firstName, lastName, gender, birthday, creationDate, locationIP, browserUsed),
	cs.Feedback(content,rate) -> kvSchema.feedback( content, rating),
	cs.Feedback( customer,product) -> kvSchema.feedback( customerid, prodid),
	cs.Order(id, orderdate, totalprice) -> docSchema.ordersCol( OrderId, OrderDate, TotalPrice),
	
	cs.write.review -> kvSchema.feedback.customer1,
	cs.has_reviews.reviews -> kvSchema.feedback.product,
	cs.buys.order -> docSchema.ordersCol.customer,
	cs.composed_of.orderP -> docSchema.ordersCol.Orderline()
}

databases {
	mariadb mysqlbench{
		dbname : "mysqlbench"
		host : "localhost"
		login : "root"
		password : "password"
		port : 3310
	}
	redis redisbench {
		host : "localhost"
		port : 6363
	}
	mongodb mongobench{
		host:"localhost"
		port : 27000
	}
}

