conceptual schema cs {
    entity type Product{
        id:int,
        name:string,
        description:string
    }
    entity type Stock{
       
    }
    
    entity type Test{
    
    }
    
    entity type Person{
    
    }
    
    relationship type hasParent{
    	child [1] : Person
    	parent [0..*] : Person
    }
    
    relationship type productStock{
        storage [0..*] : Stock
        stored_products [1] : Product
    }
    
    relationship type testrel {
    	aaa [0..*] : Product
    	bbb [1] : Stock
    	ccc [0..*] : Test
    	ddd [1] : Test
    	test : date
    }
}
physical schemas {
    document schema myDocSchema{
        collection myCollection{
            fields {
           
            }
        }
    }
    key value schema kvschema {}
    key value schema fd{}
}
mapping rules {
}