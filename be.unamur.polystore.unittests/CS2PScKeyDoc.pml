//spark
conceptual schema CS2PScKeyDoc {
	
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
				locationip,
				birthday,
				gender,
				creationdate,
				browser
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
	CS2PScKeyDoc.Customer(id,firstname, lastname, locationip, birthday, gender, creationDate, browser) -> kv.customerKV(customerid, firstname,lastname, locationip, birthday, gender, creationdate, browser),
	CS2PScKeyDoc.Customer(id) -> kv.customerOrdersRefs( custid),
	CS2PScKeyDoc.Order(id,orderdate,totalprice) -> mongoSchema.ordersCol(OrderId,OrderDate,TotalPrice),
	CS2PScKeyDoc.buys.client -> kv.customerOrdersRefs.purchases
}

databases {
		mongodb mongoPerfTest{
		host:"138.48.33.187"
		port : 27701
	}
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
	