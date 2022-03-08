//spark
conceptual schema CS1PSbDoc {
	
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
	CS1PSbDoc.Order(id,orderdate) -> mongoSchema.ordersColA(OrderId,OrderDate),
	CS1PSbDoc.Order(id,totalprice) -> mongoSchema.ordersColB(id,TotalPrice)
}

databases {
		mongodb mongoPerfTest{
		host:"localhost"
		port : 27701
	}
}
	