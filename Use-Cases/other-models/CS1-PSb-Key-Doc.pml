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
		kvpairs ordersPairsA {
			key : "ORDER:"[orderid]"DATEORDER",
			value :	dateOrder
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
	cs.Order(id, orderdate) -> kv.ordersPairsA(orderid,dateOrder)
}

databases {
		mongodb mongoPerfTest{
		host:"localhost"
		port : 27701
	}
	redis redisPerfTest {
		host : "localhost"
		port : 6364
	}
}
	