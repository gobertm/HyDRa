//spark
conceptual schema CS3PSbRelKey {
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
			key : "ORDER:"[idorder],
			value : hash {
				dateOrder,
				amount
			}
		}
	
	}
	
	relational schema relSchema : mysqlPerfTest {
		
		table productTable{
			columns{
				asin,
				title,
				price,
				imgUrl
			}
		}

	table detailOrderTable{
			columns{
				order_id,
				product_id
			}
			
			references {
				orderRef : order_id -> kv.ordersPairs.idorder
				productRef : product_id -> productTable.asin
			}
		}
	}
	
}

mapping rules
{
	CS3PSbRelKey.Order(id,totalprice, orderdate) -> kv.ordersPairs(idorder,amount,dateOrder),
	CS3PSbRelKey.Product(id, title, price,photo) -> relSchema.productTable(asin, title, price, imgUrl),
	CS3PSbRelKey.composed_of.orderP -> relSchema.detailOrderTable.orderRef,
	CS3PSbRelKey.composed_of.orderedProducts -> relSchema.detailOrderTable.productRef
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