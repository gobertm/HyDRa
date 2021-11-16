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
	relational schema relSchemaB : mysqlModelB {
		table orderTable {
			columns {
				orderId,
				orderDate,
				customerId
			}
			
			references {
				clientRef: customerId->mongoSchemaB.userCol.id
			}
		}
	}
	
	document schema mongoSchemaB : mongoModelB {
		collection userCol{
			fields {
				id,
				firstName,
				lastName,
				gender,
				birthday,
				creationDate,
				locationIP,
				browserUsed,
				place,
				orders[0-N]{
					id,
					buydate,
					totalamount
				}			
			}
		}
		
		collection detailOrderCol{
			fields{
				orderid,
				productid
			}
			
			references {
				orderRef : orderid -> relSchemaB.orderTable.orderId
				productRef : productid -> kvSchemaB.products.asin
			}
		}
	}
	
	key value schema kvSchemaB : redisModelB {
		
		kvpairs products {
			key : "PRODUCT:"[asin],
			value : hash {
				asin,
				title,
				price,
				imgUrl	
			}
		}
		
		kvpairs feedback {
			key : [prodid]":"[customerid],
			value : [rating]"&&"[content]
			references {
				product : prodid -> products.asin
				writer : customerid -> mongoSchemaB.userCol.id
			}
		}
	}
}

mapping rules {
	cs.Product(id, title, price,photo) -> kvSchemaB.products(asin, title, price, imgUrl),
	cs.Customer(id,firstname,lastname, gender, birthday, creationDate, locationip, browser) -> mongoSchemaB.userCol(id, firstName, lastName, gender, birthday, creationDate, locationIP, browserUsed),
	cs.Feedback(content,rate) -> kvSchemaB.feedback( content, rating),
	cs.Feedback( customer,product) -> kvSchemaB.feedback( customerid, prodid),
	cs.Order(id, orderdate) -> relSchemaB.orderTable( orderId, orderDate),
	cs.Order(id, orderdate, totalprice) -> mongoSchemaB.userCol.orders( id, buydate, totalamount),

	cs.write.review -> kvSchemaB.feedback.writer,
	cs.has_reviews.reviews -> kvSchemaB.feedback.product,
	cs.composed_of.orderP -> mongoSchemaB.detailOrderCol.orderRef,
	cs.composed_of.orderedProducts -> mongoSchemaB.detailOrderCol.productRef,
	cs.buys.order -> relSchemaB.orderTable.clientRef,
	cs.buys.client -> mongoSchemaB.userCol.orders()
}

databases {
	mariadb mysqlModelB{
		dbname : "mysqlModelB"
		host : "localhost"
		login : "root"
		password : "password"
		port : 3333
	}
	redis redisModelB {
		host : "localhost"
		port : 6300
	}
	mongodb mongoModelB{
		host:"localhost"
		port : 27700
	}
}

