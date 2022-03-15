//spark
conceptual schema CS1PSaDoc {
	
	entity type Order {
		id : string,
		orderdate : date,
		totalprice : float
		identifier {
			id
		}
	}
	
}

physical schemas {
	document schema mongoSchema0: mongoPerfTest {
			
		collection ordersCol {
			fields {
				OrderId,
				PersonId,
				OrderDate,
				TotalPrice,
				Orderline[0-N] {
					asin,
					title,
					price
				}
			}
		}
	
	}
}
mapping rules {
	CS1PSaDoc.Order(id,orderdate,totalprice) -> mongoSchema0.ordersCol(OrderId,OrderDate,TotalPrice)
}

databases {
		mongodb mongoPerfTest{
		host:"mongoPerfTest"
		port : 27017
	}
}
	