spark
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
	cs.Order(id,totalprice, orderdate) -> kv.ordersPairs(orderid,amount,dateOrder)
}

databases {
	redis redisPerfTest {
		host : "localhost"
		port : 6364
	}
}
	