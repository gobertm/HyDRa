//spark
conceptual schema CS3PSbKeyRel {
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
	
	relationship type composed_of {
		orderP[1-N] : Order,
		orderedProducts[0-N] : Product
	}
	
}

physical schemas {
	
	relational schema relSchema : mysqlPerfTest {
		table orderTable {
			columns {
				orderId,
				orderDate,
				totalamount
				}
		}
	}
	
	key value schema kv: redisPerfTest {
		
//		kvpairs ordersPairs {
//			key : "ORDER:"[orderid],
//			value : hash {
//				dateOrder,
//				amount
//			}
//		}
		
		
		kvpairs productPairs {
			key:"PRODUCT:"[prodid],
			value : hash {
				title,
				price,
				imgUrl
			}
		}
		
		kvpairs ordersProducts {
			key : "ORDER:"[orderid]":PRODUCTS",
			value : list {
				productid
			}
			references {
				bought : productid -> productPairs.prodid
			}
		}
		
		kvpairs productOrders {
			key : "PRODUCT:"[prodid]":ORDERS",
			value : list {
				order
			}
			references {
				article_in : order -> relSchema.orderTable.orderId
			}
		}
		
	}
}

mapping rules
{
	CS3PSbKeyRel.Order(id,totalprice, orderdate) -> relSchema.orderTable(orderId,totalamount,orderDate),
	CS3PSbKeyRel.Order(id) -> kv.ordersProducts(orderid),
	CS3PSbKeyRel.Product(id, title, price, photo) -> kv.productPairs(prodid,title,price,imgUrl),
	CS3PSbKeyRel.Product(id) -> kv.productOrders(prodid),
	CS3PSbKeyRel.composed_of.orderedProducts -> kv.productOrders.article_in,
	CS3PSbKeyRel.composed_of.orderP -> kv.ordersProducts.bought
}

databases {
		mongodb mongoPerfTest{
		host:"localhost"
		port : 27701
	}
			mysql mysqlPerfTest{
		dbname : "mysqlPerfTest"
		host : "localhost"
		login : "root"
		password : "password"
		port : 3334
	}
		redis redisPerfTest {
		host : "localhost"
		port : 6364
	}
}