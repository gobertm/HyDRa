//spark
conceptual schema CS1PSbRelKey {
	
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
	
	key value schema kv : redisPerfTest {

		kvpairs ordersPairsB {
			key : "ORDER:"[orderid]":AMOUNT",
			value : amount
		}
	}
	
}
mapping rules {
	CS1PSbRelKey.Order(id, orderdate) -> relSchema.orderTableA(orderId, orderDate),
	CS1PSbRelKey.Order(id,totalprice) -> kv.ordersPairsB(orderid,amount)
}

databases {
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
	}
}
	