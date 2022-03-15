//spark
conceptual schema CS1PSaRel {
	
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
		table orderTable {
			columns {
				orderId,
				orderDate,
				totalamount,
				customerId
			}
		}
	}
}
mapping rules {
	CS1PSaRel.Order(id, orderdate, totalprice) -> relSchema.orderTable( orderId, orderDate, totalamount)
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
	