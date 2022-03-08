//spark
conceptual schema CS1PSbKey {
	
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
	key value schema kv : redisPerfTest {
		kvpairs ordersPairsA {
			key : "ORDER:"[orderid]"DATEORDER",
			value :	dateOrder
		}
		kvpairs ordersPairsB {
			key : "ORDER:"[orderid]":AMOUNT",
			value : amount
		}
	}
}
mapping rules {
	CS1PSbKey.Order(id, orderdate) -> kv.ordersPairsA(orderid,dateOrder),
	CS1PSbKey.Order(id,totalprice) -> kv.ordersPairsB(orderid,amount)
}

databases {
	redis redisPerfTest {
		host : "localhost"
		port : 6364
	}
}
	