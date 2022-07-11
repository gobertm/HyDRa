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
	
}

physical schemas {
	document schema mongoSchema0: mongoPerfTest {
			
		collection ordersCol {
			fields {
				OrderId,
				OrderDate,
				TotalPrice
			}
		}
	
	}
}
mapping rules {
	cs.Order(id,orderdate,totalprice) -> mongoSchema0.ordersCol(OrderId,OrderDate,TotalPrice)
}

databases {
		mongodb mongoPerfTest{
		host:"localhost"
		port : 27701
	}
}
	