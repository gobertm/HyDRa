//spark
conceptual schema cs {
	
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
	cs.Order(id,totalprice) -> mongoSchema.ordersColB(id,TotalPrice),
	cs.Order(id, orderdate) -> relSchema.orderTableA(orderId, orderDate)
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
}
	