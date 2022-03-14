//spark
conceptual schema CS3PSaKey {
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
	key value schema kv: redisPerfTest {
		
		kvpairs ordersPairs {
			key : "ORDER:"[orderid],
			value : hash {
				dateOrder,
				amount
			}
		}
		
		
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
				article_in : order -> ordersPairs.orderid
			}
		}
		
	}
}

mapping rules
{
	//KV
	CS3PSaKey.Order(id,totalprice, orderdate) -> kv.ordersPairs(orderid,amount,dateOrder),
	CS3PSaKey.Order(id) -> kv.ordersProducts(orderid),
	CS3PSaKey.Product(id, title, price, photo) -> kv.productPairs(prodid,title,price,imgUrl),
	CS3PSaKey.Product(id) -> kv.productOrders(prodid),
	CS3PSaKey.composed_of.orderedProducts -> kv.productOrders.article_in,
	CS3PSaKey.composed_of.orderP -> kv.ordersProducts.bought
}

databases {
		mongodb mongoPerfTest{
		host:"138.48.33.187"
		port : 27701
	}
			mysql mysqlPerfTest{
		dbname : "mysqlPerfTest"
		host : "138.48.33.187"
		login : "root"
		password : "password"
		port : 3334
	}
		redis redisPerfTest {
		host : "138.48.33.187"
		port : 6364
//		port : 6366
	}
}