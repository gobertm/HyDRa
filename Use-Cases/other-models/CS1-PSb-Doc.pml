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
	document schema mongoSchema: mongoPerfTest {
			
		collection ordersColA {
			fields {
				OrderId,
				PersonId,
				OrderDate
			}
		}
		
		collection ordersColB {
			fields {
				id,
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
	cs.Order(id,orderdate) -> mongoSchema.ordersColA(OrderId,OrderDate),
	cs.Order(id,totalprice) -> mongoSchema.ordersColB(id,TotalPrice)
}

databases {
		mongodb mongoPerfTest{
		host:"localhost"
		port : 27701
	}
}
	