//spark
conceptual schema CS3PSaRel {
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
		table orderTable {
			columns {
				orderId,
				orderDate,
				totalamount,
				customerId
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
				order_id,
				product_id
			}
			
			references {
				orderRef : order_id -> relSchema.orderTable.orderId
				productRef : product_id -> productTable.asin
			}
		}
	}
}

mapping rules
{
	CS3PSaRel.Product(id, title, price,photo) -> relSchema.productTable(asin, title, price, imgUrl),
	CS3PSaRel.Order(id, orderdate, totalprice) -> relSchema.orderTable(orderId, orderDate, totalamount),
	CS3PSaRel.composed_of.orderP -> relSchema.detailOrderTable.orderRef,
	CS3PSaRel.composed_of.orderedProducts -> relSchema.detailOrderTable.productRef
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