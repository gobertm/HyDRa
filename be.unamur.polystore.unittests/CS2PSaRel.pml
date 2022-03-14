//spark
conceptual schema CS2PSaRel {
	
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
				clientRef: customerId->customerTable.id
			}
		}
		
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
	CS2PSaRel.Order(id, orderdate, totalprice) -> relSchema.orderTable( orderId, orderDate, totalamount),
	CS2PSaRel.Customer(id,firstname,lastname, gender, birthday, creationDate, locationip, browser) -> relSchema.customerTable(id, firstName, lastName, gender, birthday, creationDate, locationIP, browserUsed),
	CS2PSaRel.buys.order -> relSchema.orderTable.clientRef	
}

databases {
	mysql mysqlPerfTest{
		dbname : "mysqlPerfTest"
		host : "138.48.33.187"
		login : "root"
		password : "password"
		port : 3334
	}
}