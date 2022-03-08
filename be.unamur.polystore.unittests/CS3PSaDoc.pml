//spark
conceptual schema CS3PSaDoc {
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
				orderRef : order -> ordersCol.OrderId
				productRef : product -> productsCol.asin
			}
		}
	
	}
}

mapping rules
{
	CS3PSaDoc.Order(id,orderdate,totalprice) -> mongoSchema.ordersCol(OrderId,OrderDate,TotalPrice),
	CS3PSaDoc.Product(id,title,price,photo) -> mongoSchema.productsCol(asin,title,price,imgUrl),
	CS3PSaDoc.composed_of.orderP -> mongoSchema.detailOrderCol.orderRef,
	CS3PSaDoc.composed_of.orderedProducts -> mongoSchema.detailOrderCol.productRef
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