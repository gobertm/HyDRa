conceptual schema conceptualSchema{
		
	entity type Product {
		id : string,
		Name : string,
        photo : string,
        price:int,
        description : string
        identifier {
        	id
        }
    }
	
	entity type Client {
		id : int,
		firstname : string,
		lastname : string,
		street : string,
		number : int
		identifier {
			id
		}
	}
	
	entity type ShoppingCart {
		id : int,
		nbArticle : int,
		last_update_time : string
	}
	
	relationship type clientShop{
		customer[1] : Client,
		cart[1] : ShoppingCart
	}
	
	relationship type cartProduct{
		booked_product[0-N]:Product,
		cart[0-N] : ShoppingCart
	}
}

physical schemas { 
	key value schema KVSchema : myredis{
		kvpairs KVProdPhotos {
			key:"PRODUCT:"[prodID]":PHOTO",
			value:photo
		}
		
		kvpairs KVClient {
			key : "CLIENT:"[clientID],
			value : attr hash { 
				name : [firstname]"_"[lastname],
				streetnumber : [streetnbr], 
				street
			}
		}
			
	}
	
	relational schema myRelSchema : mydb {
		table ProductCatalogTable {
			columns {
				product_id,
				europrice : [price]"€",
				description,
				categoryname
			}
		}
	}
	
	
}

mapping rules{
	conceptualSchema.Product(id,photo) -> KVSchema.KVProdPhotos(prodID,photo),
	conceptualSchema.Client(id) -> KVSchema.KVClient(clientID),
	conceptualSchema.Client(firstname,lastname,street,number) -> KVSchema.KVClient.attr(firstname,lastname,street,streetnbr),
	conceptualSchema.Product(id,price,description) -> myRelSchema.ProductCatalogTable(product_id,price,description)
}

databases {
	redis myredis{
		host:"localhost"
		port:6363
	}
	
	mariadb mydb {
		host: "localhost"
		port: 3307
		dbname : "mydb"
		password : "password"
		login : "root"
	}
}


