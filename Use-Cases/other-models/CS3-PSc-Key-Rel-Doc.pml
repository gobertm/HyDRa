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
				orderid,
				productid
			}
			
			references {
				orderRef : orderid -> kv.ordersPairs.idorder
				productRef : productid -> productTable.asin
			}
		}
	}

}

mapping rules
{
	cs.Order(id,totalprice, orderdate) -> kv.ordersPairs(idorder,amount,dateOrder),
	cs.Product(id, title, price,photo) -> relSchema.productTable(asin, title, price, imgUrl),
	cs.composed_of.orderP -> relSchema.detailOrderTable.orderRef,
	cs.composed_of.orderedProducts -> relSchema.detailOrderTable.productRef
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