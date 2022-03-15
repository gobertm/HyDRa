//spark
conceptual schema CS2PSbRelDoc {
	
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
				clientRef: customerId->mongoSchema.userCol.id
			}
		}
	}
	
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
				orders[0-N]{
					id,
					buydate,
					totalamount
				}			
			}
		}
	
	}
}
mapping rules {
	CS2PSbRelDoc.Order(id, orderdate, totalprice) -> relSchema.orderTable( orderId, orderDate, totalamount),
	CS2PSbRelDoc.Customer(id,firstname,lastname, gender, birthday, creationDate, locationip, browser) -> mongoSchema.userCol(id, firstName, lastName, gender, birthday, creationDate, locationIP, browserUsed),
	CS2PSbRelDoc.buys.order -> relSchema.orderTable.clientRef	
}

databases {
	mysql mysqlPerfTest{
		dbname : "mysqlPerfTest"
		host : "mysqlPerfTest"
		login : "root"
		password : "password"
		port : 3306
	}
	mongodb mongoPerfTest{
		host:"mongoPerfTest"
		port : 27017
	}
}