conceptual schema conceptualSchema{

	entity type Customer {
		id : int,
		firstName : string,
		lastName : string,
		address : string
		identifier {
			id
		}
	}
	
	entity type Order {
		id : int,
		quantity: int
		identifier {
			id
		}
	}
	
	entity type Product {
		id : int,
		label : string,
		price : float
		identifier{
			id
		}
	}
	
	entity type Store {
		id : int,
		VAT : string,
		address : string
		identifier{
			id
		}
	}
	   
    relationship type places{
		buyer[0-N]: Customer,
		order[1] : Order
	}
	
	relationship type of{
		bought_item[0-N]: Product,
		order[1] : Order
	}  
	
	relationship type from{
		store[0-N]: Store,
		order[1] : Order
	}   
}
physical schemas { 
	
	document schema mongoSchema : mongo {
		collection ClientCollection {
			fields {
				id,
				fullName: [firstName]" "[lastName],
				postalAddress,
				orders[0-N]{
					orderId,
					qty,
					productId,
					storeId
				}
			}
			
			references {
				buys : orders.productId -> mysqlSchema.PRODUCT.ID
				buys_in : orders.storeId -> mysqlSchema.STORE.ID
			}
		}
	}
	
	relational schema mysqlSchema : INVENTORY {
		table PRODUCT{
			columns{
				ID,
				NAME,
				PRICE
			}
		}
		
		
		table STORE {
			columns{
				ID,
				VAT,
				ADDR
			}
			
		}
	}
}

mapping rules{
	conceptualSchema.Customer(id,firstName,lastName,address) -> mongoSchema.ClientCollection(id,firstName,lastName,postalAddress),
	conceptualSchema.Order(id,quantity) -> mongoSchema.ClientCollection.orders(orderId, qty),
	conceptualSchema.Product(id,label,price) -> mysqlSchema.PRODUCT(ID,NAME,PRICE),
	conceptualSchema.Store(id,VAT,address) -> mysqlSchema.STORE(ID,VAT,ADDR),
	
	conceptualSchema.places.buyer-> mongoSchema.ClientCollection.orders(),
	conceptualSchema.of.order-> mongoSchema.ClientCollection.buys,
	conceptualSchema.from.order-> mongoSchema.ClientCollection.buys_in
}

databases {
	
	mariadb INVENTORY {
		host: "localhost"
		port: 3307
		dbname : "INVENTORY"
		password : "password"
		login : "root"
	}
	
	mongodb mongo{
		host : "localhost"
		port: 27100
	}
	
}