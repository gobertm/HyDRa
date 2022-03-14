//spark
conceptual schema CS2PSbKeyRel {
	
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
		CS2PSbKeyRel.Order(id,totalprice, orderdate) -> kv.ordersPairs(orderid,amount,dateOrder),
		CS2PSbKeyRel.Customer(id,firstname,lastname, gender, birthday, creationDate, locationip, browser) -> relSchema.customerTable(id, firstName, lastName, gender, birthday, creationDate, locationIP, browserUsed),	
		CS2PSbKeyRel.buys.order -> kv.ordersPairs.clientRefKV
}

databases {
	redis redisPerfTest {
		host : "138.48.33.187"
		port : 6364
	}
		mysql mysqlPerfTest{
		dbname : "mysqlPerfTest"
		host : "138.48.33.187"
		login : "root"
		password : "password"
		port : 3334
	}
}