conceptual schema cs {
    entity type Product{
        id:int,
        name:string,
        description:string
    }
    entity type Stock{
       
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
           
            }
        }
    }
    key value schema kvschema {}
    key value schema fd{}
}
mapping rules {
  cs.Product -> kvschema
}