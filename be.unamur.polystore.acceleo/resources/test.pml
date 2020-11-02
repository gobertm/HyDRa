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
    	parent [0-N] : Person,
    	officialDate : date
    }
    
    relationship type productStock{
        storage [0-N] : Stock
        stored_products [1] : Product
    }
    
    relationship type testrel {
    	aaa [0-N] : Product
    	bbb [1] : Stock
    	ccc [0-N] : Test
    	ddd [1] : Test,
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
   
   	key value schema KVProject {
		
		kvpairs KVProjDesc {
			key : "PROJECT:""dzd"[IDPROJ], // Mais "string"[ID]"ojd" does not work.
			value : [Description]
		}
		
	}
   
}
mapping rules {
}