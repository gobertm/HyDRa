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
	
	relationship type composed_of {
		orderP[1-N] : Order,
		orderedProducts[0-N] : Product
	}
	
}

physical schemas {
	document schema mongoSchema: mongoPerfTest {
			
		collection ordersCol {
			fields {
				OrderId,
				PersonId,
				OrderDate,
				TotalPrice
			}
		}
	}
	
key value schema kv: redisPerfTest {
		
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
				article_in : order -> mongoSchema.ordersCol.OrderId
			}
		}
		
	}
}

mapping rules
{
	cs.Order(id,orderdate,totalprice) -> mongoSchema.ordersCol(OrderId,OrderDate,TotalPrice),
	cs.Order(id) -> kv.ordersProducts(orderid),
	cs.Product(id, title, price, photo) -> kv.productPairs(prodid,title,price,imgUrl),
	cs.Product(id) -> kv.productOrders(prodid),
	cs.composed_of.orderedProducts -> kv.productOrders.article_in,
	cs.composed_of.orderP -> kv.ordersProducts.bought
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