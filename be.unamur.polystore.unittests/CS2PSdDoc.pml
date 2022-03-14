//spark
conceptual schema CS2PSdDoc {
	
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
	CS2PSdDoc.Order(id,orderdate,totalprice) -> mongoSchema.userCol.orders(id,buydate,totalamount),
	CS2PSdDoc.Customer(id,firstname,lastname, gender, birthday, creationDate, locationip, browser) -> mongoSchema.userCol(id, firstName, lastName, gender, birthday, creationDate, locationIP, browserUsed),
	CS2PSdDoc.buys.client -> mongoSchema.userCol.orders()
}

databases {
		mongodb mongoPerfTest{
		host:"138.48.33.187"
		port : 27701
	}
}
	