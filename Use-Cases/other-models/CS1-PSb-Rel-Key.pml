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
	
	key value schema kv : redisPerfTest {

		kvpairs ordersPairsB {
			key : "ORDER:"[orderid]":AMOUNT",
			value : amount
		}
	}
	
}
mapping rules {
	cs.Order(id, orderdate) -> relSchema.orderTableA(orderId, orderDate),
	cs.Order(id,totalprice) -> kv.ordersPairsB(orderid,amount)
}

databases {
	mysql mysqlPerfTest{
		dbname : "mysqlPerfTest"
		host : "localhost"
		login : "root"
		password : "password"
		port : 3334
	}
	
	redis redisPerfTest {
		host : "localhost"
		port : 6364
	}
}
	