//spark
conceptual schema CS3PSbRelDoc {
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
				order_id,
				product_id
			}
			
			references {
				orderRef : order_id -> mongoSchema.ordersCol.OrderId
				productRef : product_id -> productTable.asin
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
	CS3PSbRelDoc.Product(id, title, price,photo) -> relSchema.productTable(asin, title, price, imgUrl),
	CS3PSbRelDoc.Order(id,orderdate,totalprice) -> mongoSchema.ordersCol(OrderId,OrderDate,TotalPrice),
	CS3PSbRelDoc.composed_of.orderP -> relSchema.detailOrderTable.orderRef,
	CS3PSbRelDoc.composed_of.orderedProducts -> relSchema.detailOrderTable.productRef
}

databases {
		mysql mysqlPerfTest{
		dbname : "mysqlPerfTest"
		host : "mysqlPerfTest"
		login : "root"
		password : "password"
		port : 3306
	}
			mongodb mongoPerfTest{
		host:"mongoPerfTest"
		port : 27017
	}
}