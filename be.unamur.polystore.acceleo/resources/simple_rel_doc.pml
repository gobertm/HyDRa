conceptual schema cs {
	entity type Product{
		id:int,
		name:string,
		price:float,
		description:string
		
		identifier{
			id
		}
	}
	
	entity type Client{
		clientnumber : int, 
		lastname : string,
		firstname : string,
		age: int
		
		identifier{
			clientnumber
		}
		
	} 
	
	entity type Review {
		id : string,
		rating : int,
		content : string
	}
	
	entity type Category {
		name : string,
		description : string
	}
	
	entity type Comment {
		comment : string,
		number : int // should be the nmber of the comment on the review (incremental)
	}
	
	relationship type productReview{
	reviews[1]: Review,
	product[0-N] : Product,
	review_date : date
	}
	
	relationship type commentReview {
		review[0-N]: Review, 
		comments[1]: Comment
	}
}
physical schemas {
	document schema myDocSchema : mymongo{
		collection productCollection{
			fields { 
				id,
				product_ref,
				productDescription,
				productprice, // A renommer 'price'
				productname, // A renommer en 'name' quand test sur EmbeddedObject done
				reviews[0-N]{
					// To test
					product_attributes [1] {
						name,
						price : [price]"$"						
						},
					userid,
					numberofstars:[rate],
					ratingstring :[rate2]"*",
					title,
					content,
					comments[0-N]{
						comment,
						number
					}
				}
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
	}
}
	
mapping rules{
	cs.Product(id,description,name,price) -> myDocSchema.productCollection(product_ref,productDescription/*,name,price*/), // Commenter pour tester les EmbeddedObject Acceleo..
	cs.productReview.reviews -> myDocSchema.productCollection.reviews(),
	cs.Product(name,price) -> myDocSchema.productCollection.reviews.product_attributes(name,price),
	cs.Review(content,rating) -> myDocSchema.productCollection.reviews(content,rate),
	cs.Review(rating) -> myDocSchema.productCollection.reviews(rate2),
	cs.commentReview.comments -> myDocSchema.productCollection.reviews.comments(),
	cs.Comment(comment) -> myDocSchema.productCollection.reviews.comments(comment),
	cs.Client(lastname,firstname,age, clientnumber) -> myRelSchema.Customer(lastName, firstName,age, clientnumber)
}

databases {
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
		host : "localhost"
		port:27000
	}
}
