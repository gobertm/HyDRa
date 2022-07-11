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
		orderedProducts[0-N] : Product,
		quantity : int
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
				product,
				quantity
			}
			
			references {
				orderRef : order -> relSchema.orderTable.orderId
				productRef : product -> productTable.asin
			}
		}
	}
}

mapping rules
{
	cs.Product(id, title, price,photo) -> relSchema.productTable(asin, title, price, imgUrl),
	cs.Order(id, orderdate, totalprice) -> relSchema.orderTable(orderId, orderDate, totalamount),
	cs.composed_of.orderP -> relSchema.detailOrderTable.orderRef,
	cs.composed_of.orderedProducts -> relSchema.detailOrderTable.productRef,
	rel : cs.composed_of(quantity) -> relSchema.detailOrderTable(quantity)
}

databases {
		mysql mysqlPerfTest{
		dbname : "mysqlPerfTest"
		host : "localhost"
		login : "root"
		password : "password"
		port : 3334
	}
}