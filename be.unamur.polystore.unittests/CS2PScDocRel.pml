//spark
conceptual schema CS2PScDocRel {
	
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
			
		collection userCol{
			fields {
				id,
				firstName,
				lastName,
				gender,
				birthday,
				creationDate,
				locationIP,
				browserUsed,
				place,
				orders[]: ordId
			}
			references{
				placed_order : ordId -> relSchema.orderTable.orderId
			}
		}
	
	}
	
	relational schema relSchema : mysqlPerfTest {
		table orderTable {
			columns {
				orderId,
				orderDate,
				totalamount,
				customerId
			}
		}
	}
}
mapping rules {
	CS2PScDocRel.Customer(id,firstname,lastname, gender, birthday, creationDate, locationip, browser) -> mongoSchema.userCol(id, firstName, lastName, gender, birthday, creationDate, locationIP, browserUsed),
	CS2PScDocRel.Order(id, orderdate, totalprice) -> relSchema.orderTable( orderId, orderDate, totalamount),
	CS2PScDocRel.buys.client -> mongoSchema.userCol.placed_order
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
}
	