//spark
conceptual schema CS1PSbKeyDoc {
	
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
	CS1PSbKeyDoc.Order(id,totalprice) -> mongoSchema.ordersColB(id,TotalPrice),
	CS1PSbKeyDoc.Order(id, orderdate) -> kv.ordersPairsA(orderid,dateOrder)
}

databases {
		mongodb mongoPerfTest{
		host:"mongoPerfTest"
		port : 27017
	}
	redis redisPerfTest {
		host : "redisPerfTest"
		port : 6379
	}
}
	