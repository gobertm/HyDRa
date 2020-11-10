conceptual schema cs {

	entity type Customer {
		custId:int,
		firstName:string,
		lastName:string,
		streetName:string,
		streetNumber:string,
		city:string,
		country:string
	}
	
	entity type Order {
		orderId:int,
		dateOrder:date
	}
	
    entity type Product{
        id:int,
        name:string,
        description:string
    }
    
    entity type Student {
    	studId:int,
    	name:string
    }
    
    entity type Project {
    	projId:int,
    	label:string
    }
    
    entity type Book {
    	bookId:int,
    	title:string,
    	author:string
    }
    
    entity type Stock{
       
    }
    
    entity type Test{
    
    }
    
    entity type Person{
    
    }
    
    relationship type hasParent{
    	child [1] : Person
    	parent [0-N] : Person,
    	officialDate : date
    }
    
    relationship type productStock{
        storage [0-N] : Stock
        stored_products [1] : Product
    }
    
    relationship type testrel {
    	aaa [0-N] : Product
    	bbb [0-N] : Stock
    	ccc [0-N] : Test
    	ddd [1] : Test,
    	test : date
    }
    
    relationship type Placing{
		placed_order [1]: Order
		buyer [0-N]: Customer
	}
	
	relationship type Borrowing{
		borrower [1]: Student
		project [0-N]: Project
		borrowedBook [0-N]: Book 
	}
}
physical schemas {
    document schema myDocSchema{
        collection myCollection{
            fields {
           
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
}