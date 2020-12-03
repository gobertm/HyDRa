conceptual schema cs {
	entity type Product{
		id:int,
		name:string,
		description:string
		
		identifier{
			id
		}
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
		firstname : string,
		age: int
		
		identifier{
			clientnumber
		}
		
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
					numberofstars:[rate],
					ratingstring :[rate2]"*",
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
				clientnumber,
				fullname:[firstName]" "[lastName],
				age:[age]" years old"
			}
		}
		table Order {
			columns{ 
				id,
				cust_id
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
					name:[first]"_"[last]
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
			key : "PROJECT:"+"dzd"+[IDPROJ]+"stringwhatevever"+[field],
			value : [Description]
		}
		
	}
}

mapping rules { 
	  cs.Product(description, id, name) -> myDocSchema.productCollection(Description,id,Name),
	  cs.productReview.reviews -> myDocSchema.productCollection(review),
	  cs.Product(id,name,description) -> myGraphSchema.Product(_id,Name,Description),
	  cs.Review(rating,content) -> myDocSchema.productCollection.review(rate,content),
	  cs.Review(rating) -> myDocSchema.productCollection.review(rate2),
	  cs.productStock.storage -> myGraphSchema.PART_OF(),
	  cs.Client(lastname,firstname,age, clientnumber) -> myRelSchema.Customer(lastName, firstName,age, clientnumber),
	  cs.Client(lastname,firstname) -> colSchema.Client(last,first)
	  
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
}
	