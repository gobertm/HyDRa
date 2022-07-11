spark
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
				order,
				product
			}
			
			references {
				orderRef : order -> mongoSchema.ordersCol.OrderId
				productRef : product -> productTable.asin
			}
		}
	}
	
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
}

mapping rules
{
	cs.Product(id, title, price,photo) -> relSchema.productTable(asin, title, price, imgUrl),
	cs.Order(id,orderdate,totalprice) -> mongoSchema.ordersCol(OrderId,OrderDate,TotalPrice),
	cs.composed_of.orderP -> relSchema.detailOrderTable.orderRef,
	cs.composed_of.orderedProducts -> relSchema.detailOrderTable.productRef
}

databases {
		mysql mysqlPerfTest{
		dbname : "mysqlPerfTest"
		host : "localhost"
		login : "root"
		password : "password"
		port : 3334
	}
			mongodb mongoPerfTest{
		host:"localhost"
		port : 27701
	}
}