//spark
conceptual schema CS1PSbRelDoc {
	
	entity type Order {
		id : string,
		orderdate : date,
		totalprice : float
		identifier {
			id
		}
	}
	
}

physical schemas {
	
	relational schema relSchema : mysqlPerfTest {
		table orderTableA {
			columns {
				orderId,
				orderDate,
				customerId
			}
		}
	}
	
	document schema mongoSchema: mongoPerfTest {
				
		collection ordersColB {
			fields {
				id,
				TotalPrice,
				Orderline[0-N] {
					asin,
					title,
					price
				}
			}
		}
	
	}
}
mapping rules {
	CS1PSbRelDoc.Order(id,totalprice) -> mongoSchema.ordersColB(id,TotalPrice),
	CS1PSbRelDoc.Order(id, orderdate) -> relSchema.orderTableA(orderId, orderDate)
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
}
	