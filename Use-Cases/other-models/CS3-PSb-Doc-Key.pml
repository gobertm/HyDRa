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
			key : "ORDER:"[orderid],
			value : hash {
				dateOrder,
				amount
			}
		}
	
	}
	
	document schema mongoSchema: mongoPerfTest {
		collection productsCol {
			fields {
				asin,
				title,
				price,
				imgUrl
			}
		}
		
		collection detailOrderCol{
			fields{
				order,
				product
			}
			
			references {
				orderRef : order -> kv.ordersPairs.orderid
				productRef : product -> productsCol.asin
			}
		}
	}
}

mapping rules
{
	cs.Order(id,totalprice, orderdate) -> kv.ordersPairs(orderid,amount,dateOrder),
	cs.Product(id,title,price) -> mongoSchema.productsCol(asin,title,price),
	cs.composed_of.orderP -> mongoSchema.detailOrderCol.orderRef,
	cs.composed_of.orderedProducts -> mongoSchema.detailOrderCol.productRef
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