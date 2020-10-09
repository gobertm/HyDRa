conceptual schema cs {
	entity type Product{
		id:int,
		name:string,
		description:string
	}
	entity type Stock{
		localisation : string
	}
	
	entity type Review {
		id : string,
		rating : int,
		content : string
	}
	
	relationship type productStock{
		storage [0-N] : Stock
		stored_products [1] : Product
	}
	relationship type productReview{
	reviews[1]: Review,
	product[0-N] : Product,
	review_date : date
	}
}
physical schemas {
	document schema myDocSchema{
		collection productCollection{
			fields { 
				id,
				Name,
				Description,
				Productnumber,
				review[0-N]{
					rate,
					content,
					comments[0-N]{
						comment
					}
				}
			}
		}
		collection StockCollection{
			fields{
				localisation,
				products[0-N]
			}
			references{
				stores : myDocSchema.StockCollection.products -> myDocSchema.productCollection.id
			}
		}
	}
	
	relational schema myRelSchema {
		table Customer {
			columns {
				id,
				name
			}
		}
		table Order {
			columns{ 
				id,
				cust_id
			}	
			references {
				 bought_by : cust_id -> myRelSchema.Customer.id
			}
		}
	}
	key value schema kvschema {}
	key value schema fd{}
}

mapping rules { 
	  cs.Product(description, id, name) -> myDocSchema.productCollection(myDocSchema.productCollection.Name, myDocSchema.productCollection.Productnumber, myDocSchema.productCollection.Description)
	}