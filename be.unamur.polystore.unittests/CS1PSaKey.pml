//spark
conceptual schema CS1PSaKey {
	
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
		kvpairs ordersPairs {
			key : "ORDER:"[orderid],
			value : hash {
				dateOrder,
				amount
			}
		}
	}
}

mapping rules {
	CS1PSaKey.Order(id,totalprice, orderdate) -> kv.ordersPairs(orderid,amount,dateOrder)
}

databases {
	redis redisPerfTest {
		host : "redisPerfTest"
		port : 6379
	}
}
	