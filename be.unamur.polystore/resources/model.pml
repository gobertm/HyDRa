conceptual schema cs {
	entity type Product{
		id:int,
		name:string,
		description:string
	}
	entity type Stock{
		localisation : string
	}
	relationship type productStock{
		storage [0..*] : Stock
		stored_products [1] : Product
	}
}
physical schema {
	document schema myDocSchema{
		collection myCollection{
			fields { 
				id,
				Name,
				Description,
				Productnumber,
				review[0..*]{
					rate,
					content,
					comments[0..*]{
						comment
					}
				}
			}
		}
		collection StockCollection{
			fields{
				localisation,
				products[0..*]
			}
			references{
				stores : myDocSchema.StockCollection.products -> myDocSchema.myCollection.id
			}
		}
	}
	key value schema kvschema {}
	key value schema fd{}
}
mapping rules { 
	  cs.Product -> kvschema
	}