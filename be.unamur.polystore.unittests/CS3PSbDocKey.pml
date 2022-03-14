//spark
conceptual schema CS3PSbDocKey {
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
	CS3PSbDocKey.Order(id,totalprice, orderdate) -> kv.ordersPairs(orderid,amount,dateOrder),
	CS3PSbDocKey.Product(id,title,price,photo) -> mongoSchema.productsCol(asin,title,price,imgUrl),
	CS3PSbDocKey.composed_of.orderP -> mongoSchema.detailOrderCol.orderRef,
	CS3PSbDocKey.composed_of.orderedProducts -> mongoSchema.detailOrderCol.productRef
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
	}
}