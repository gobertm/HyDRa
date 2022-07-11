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
	
	entity type Customer {
		id : string,
		firstname : string,
		lastname : string,
		gender : string,
		birthday : date,
		creationDate : date,
		locationip : string,
		browser : string
		identifier{
			id
		}
	}
	
	relationship type buys {
		order[1]: Order,
		client[0-N]: Customer
	}	
}

physical schemas {
	document schema mongoSchema: mongoPerfTest {
			
		collection ordersCol {
			fields {
				OrderId,
				PersonId,
				OrderDate,
				TotalPrice,
				Orderline[0-N] {
					asin,
					title,
					price
				}
			}
			references {
				clientRef : PersonId -> kv.customerKV.customerid
			}
		}
	}
	
	key value schema kv : redisPerfTest {
		kvpairs customerKV {
			key : "CUSTOMER:"[customerid],
			value : hash {
				firstname,
				lastname,
				locationip
			}
		}
	}
}
mapping rules {
	cs.Order(id,orderdate,totalprice) -> mongoSchema.ordersCol(OrderId,OrderDate,TotalPrice),
	cs.Customer(id,firstname, lastname, locationip) -> kv.customerKV(customerid, firstname,lastname, locationip),
	cs.buys.order -> mongoSchema.ordersCol.clientRef
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
	