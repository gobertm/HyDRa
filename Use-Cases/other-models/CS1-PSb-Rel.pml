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
		
		table orderTableB {
			columns {
				id,
				orderAmount
			}
		}
	}
}
mapping rules {
	cs.Order(id, orderdate) -> relSchema.orderTableA(orderId, orderDate),
	cs.Order(id, totalprice) -> relSchema.orderTableB(id,orderAmount)
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
	