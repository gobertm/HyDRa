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
	key value schema kv : redisPerfTest {
		kvpairs ordersPairs {
			key : "ORDER:"[orderid],
			value : hash {
				dateOrder,
				amount,
				custid
			}
			references {
				clientRefKV : custid-> relSchema.customerTable.id
			}
		}
	}
		relational schema relSchema : mysqlPerfTest {
		table customerTable {
			columns {
				id,
				firstName,
				lastName,
				gender,
				birthday,
				creationDate,
				locationIP,
				browserUsed,
				place
			}
		}
	}
}

mapping rules {
		cs.Order(id,totalprice, orderdate) -> kv.ordersPairs(orderid,amount,dateOrder),
		cs.Customer(id,firstname,lastname, gender, birthday, creationDate, locationip, browser) -> relSchema.customerTable(id, firstName, lastName, gender, birthday, creationDate, locationIP, browserUsed),	
		cs.buys.order -> kv.ordersPairs.clientRefKV
}

databases {
	redis redisPerfTest {
		host : "localhost"
		port : 6364
	}
		mysql mysqlPerfTest{
		dbname : "mysqlPerfTest"
		host : "localhost"
		login : "root"
		password : "password"
		port : 3334
	}
}