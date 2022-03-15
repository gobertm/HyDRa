//spark
conceptual schema CS1PSbRel {
	
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
	CS1PSbRel.Order(id, orderdate) -> relSchema.orderTableA(orderId, orderDate),
	CS1PSbRel.Order(id, totalprice) -> relSchema.orderTableB(id,orderAmount)
}

databases {
	mysql mysqlPerfTest{
		dbname : "mysqlPerfTest"
		host : "mysqlPerfTest"
		login : "root"
		password : "password"
		port : 3306
	}
}
	