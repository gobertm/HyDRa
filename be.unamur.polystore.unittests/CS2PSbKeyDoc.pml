//spark
conceptual schema CS2PSbKeyDoc {
	
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
				clientRefKV : custid-> mongoSchema.userCol.id
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
		CS2PSbKeyDoc.Order(id,totalprice, orderdate) -> kv.ordersPairs(orderid,amount,dateOrder),
		CS2PSbKeyDoc.Customer(id,firstname,lastname, gender, birthday, creationDate, locationip, browser) -> mongoSchema.userCol(id, firstName, lastName, gender, birthday, creationDate, locationIP, browserUsed),
		CS2PSbKeyDoc.buys.order -> kv.ordersPairs.clientRefKV
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
		mongodb mongoPerfTest{
		host:"138.48.33.187"
		port : 27701
	}
}