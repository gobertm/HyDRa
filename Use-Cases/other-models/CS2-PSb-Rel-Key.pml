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
		relational schema relSchema : mysqlPerfTest {
		table orderTable {
			columns {
				orderId,
				orderDate,
				totalamount,
				customerId
			}
			references {
				clientRef: customerId->kv.customerKV.customerid
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
	cs.Order(id, orderdate, totalprice) -> relSchema.orderTable( orderId, orderDate, totalamount),
	cs.Customer(id,firstname, lastname, locationip) -> kv.customerKV(customerid, firstname,lastname, locationip),
	cs.buys.order -> relSchema.orderTable.clientRef	
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