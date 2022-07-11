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
				TotalPrice
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
		
		kvpairs customerOrdersRefs {
			key : "CUSTOMER_ORDERS:"[custid],
			value : list {
				reforderid
			}
			references {
				purchases : reforderid -> mongoSchema.ordersCol.OrderId
			}
		}
	}
}
mapping rules {
	cs.Customer(id,firstname, lastname, locationip) -> kv.customerKV(customerid, firstname,lastname, locationip),
	cs.Customer(id) -> kv.customerOrdersRefs( custid),
	cs.Order(id,orderdate,totalprice) -> mongoSchema.ordersCol(OrderId,OrderDate,TotalPrice),
	cs.buys.client -> kv.customerOrdersRefs.purchases
}

databases {
		mongodb mongoPerfTest{
		host:"localhost"
		port : 27701
	}
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
	