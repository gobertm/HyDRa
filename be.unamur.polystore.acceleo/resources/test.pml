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
	
	entity type Client{
		clientnumber : int, // auto increment id. On en a besoin pour l'exemple bitmap key value.
		lastname : string,
		firstname : string
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
	document schema myDocSchema : mymongo{
		collection productCollection{
			fields { 
				id,
				Name,
				Description,
				Productnumber,
				review[0-N]{
					rate,
					numberofstars:[rate],
					cv numberofstarts:[rate]+"*",
					content,
					comments[0-N]{
						comment
					}
				}
			}
		}
		collection StockCollection{
			fields{
				_id,
				localisation,
				products[0-N]
			}
			references{
				stores : myDocSchema.StockCollection.products -> myDocSchema.productCollection.id
			}
		}
	}
	
	relational schema myRelSchema : myMariaDB, mysqlite {
		table Customer {
			columns {
				id,
				fullname:[lastname]
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
	
	graph schema myGraphSchema {
		Node Product {
			_id,
			Name,
			Description,
			StockID
		}
		
		Node Category {
			_Id,
			CategoryName
		}
		
		Edge PART_OF {
			Product -> Category,
			quantity // whatever attribute
		}
		
		references {
			stored_in : Product.StockID -> myDocSchema.StockCollection._id

		}
	}
	
	column schema colSchema {
		
		table Client {
			rowkey{
				clientnumber
			}
			
			columnfamilies {
				personnal {
					name,
					firstname,
					lastname
					}
				address{
					street,
					number,
					zipcode
				}
			}
		}
	}
	
	key value schema KVProject {
		
		kvpairs KVProjDesc {
			key : "PROJECT:""dzd"[IDPROJ], // Mais "string"[ID]"ojd" does not work.
			value : [Description]
		}
		
	}
}

mapping rules { 
	  cs.Product(description, id, name) -> myDocSchema.productCollection(Description,id,Name),
	  cs.productReview.reviews -> myDocSchema.productCollection(review),
	  cs.Product(id,name,description) -> myGraphSchema.Product(_id,Name,Description),
	  cs.productStock.storage -> myGraphSchema.PART_OF(),
	  cs.Client(clientnumber,lastname) -> myRelSchema.Customer(id,lastname)
	  
	}
	
databases{
	mariadb myMariaDB {
		host : "192.168.1.9"
		port : 3396
		dbname : "db1"
		login : "user1"
		password : "pass1"
	}
	
	sqlite mysqlite {
		host: "sqlite.unamur.be"
		port: 8090
	}
	
	mongodb mymongo {
		host: "mongo.unamur.be"
		port: 8091
	}
}
	