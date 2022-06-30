conceptual schema csmodel {
	

entity type Orders {
	id : int,
   OrderDate : date,
   RequiredDate : date,
   ShippedDate : date,
   Freight : float,
   ShipName : string,
   ShipAddress : string,
   ShipCity : string,
   ShipRegion : string,
   ShipPostalCode : string,
   ShipCountry : string
   identifier{
   	id
   }
}


entity type Products {
   productId : int,
   ProductName : string,
   QuantityPerUnit : string,
   UnitPrice : float,
   UnitsInStock : int,
   UnitsOnOrder : int,
   ReorderLevel : int,
   Discontinued : bool
   identifier{
   	productId
   }
}

entity type Suppliers {
   supplierId : int,
   CompanyName : string,
   ContactName : string,
   ContactTitle : string,
   Address : string,
   City : string,
   Region : string,
   PostalCode : string,
   Country : string,
   Phone : string,
   Fax : string,
   HomePage : text
   identifier{
   	supplierId
   }
}

entity type Customers {
   customerID : string,
   CompanyName : string,
   ContactName : string,
   ContactTitle : string,
   Address : string,
   City : string,
   Region : string,
   PostalCode : string,
   Country : string,
   Phone : string,
   Fax : string
   identifier {
   	customerID
   }
}

entity type Categories {
   categoryID : int,
   CategoryName : string,
   Description : text,
   Picture : blob
   identifier {
   	categoryID
   }
}

entity type Shippers {
   shipperID : int,
   CompanyName : string,
   Phone : string
   identifier{
   	shipperID
   }
}

entity type Employees {
   employeeID : int,
   LastName : string,
   FirstName : string,
   Title : string,
   TitleOfCourtesy : string,
   BirthDate : date,
   HireDate : date,
   Address : string,
   City : string,
   Region : string,
   PostalCode : string,
   Country : string,
   HomePhone : string,
   Extension : string,
   Photo : blob,
   Notes : text,
   PhotoPath : string,
   Salary : float
   identifier{
   	employeeID
   }
}


entity type Region {
   regionID : int,
   RegionDescription : string
   identifier{
   	regionID
   }
}

entity type Territories {
   territoryID : string,
   TerritoryDescription : string
   identifier {
   	territoryID
   }
}

relationship type locatedIn {
	territories[1] : Territories, 
	region[0-N] : Region
}

relationship type works {
	employed[0-N] : Employees,
	territories[0-N] : Territories 
}

relationship type reportsTo{
	subordonee[0-1] : Employees,
	boss[0-N] : Employees
}

relationship type supply {
	suppliedProduct[0-1] : Products,
	supplier[0-N] : Suppliers
}

relationship type typeOf {
	product[0-1] : Products,
	category[0-N] : Categories 
}

relationship type buy {
	boughtOrder[1] : Orders,
	customer[0-N] : Customers 
}

relationship type register {
	processedOrder[1] : Orders,
	employeeInCharge[0-N] : Employees 
}

relationship type ships {
	shippedOrder[1] : Orders,
	shipper[0-N] : Shippers
}

relationship type composedOf {
	order[0-N] : Orders,
	orderedProducts[0-N] : Products,
	UnitPrice : float,
	Quantity : int,
	Discount : float 
}

}
physical schemas {
	document schema mongoDB : myMongoDB{
		collection Orders {
			fields{
		   	OrderID,
		   	OrderDate,
		    RequiredDate,
		   	ShippedDate,
		   	Freight,
		   	ShipName,
		   	ShipAddress,
		   	ShipCity,
		   	ShipRegion,
		   	ShipPostalCode,
		   	ShipCountry,
		   	EmployeeRef,
		   	ShipVia,
			customer [1] {
			   CustomerID,
			   ContactName
				},
			products[1-N]{
				ProductID,
			   	ProductName,
			   	UnitPrice,
				Quantity,
				Discount
				}
			}
			
			references{
				encoded : EmployeeRef -> relDB.Employees.EmployeeID
				deliver : ShipVia -> relDB.Shippers.ShipperID
			}
		}
		
		
		collection Region {
			fields{
			   RegionID,
			   RegionDescription
			}
		}

		collection Territories {
			fields{
	 	  		TerritoryID,
			   	TerritoryDescription,
			   	RegionRef
			}
			references{
				located : RegionRef -> Region.RegionID
			}
		}
		
		collection EmployeeTerritories {
			fields {
				EmployeeRef,
				TerritoryRef
			}
			references{
				employee : EmployeeRef -> relDB.Employees.EmployeeID
				territory : TerritoryRef -> Territories.TerritoryID
			}
		}
		
	}
	
	key value schema kvDB : myRedisDB{
		
		kvpairs EmployeeOrders {
			key : "EMPLOYEE:"[emplid]":ORDERS",
			value : list {
				orderref
			}
			references {
				seller : orderref -> mongoDB.Orders.OrderID
			}
		}
		
		kvpairs Suppliers{
			key : "SUPPLIERS:"[SupplierID],
			value : hash {
			   CompanyName,
			   ContactName,
			   ContactTitle,
			   Address,
			   City,
			   Region,
			   PostalCode,
			   Country,
			   Phone,
			   Fax,
			   HomePage
			}
		}
		
		kvpairs categoriesKV {
			key : "CATEGORY:"[catid],
			value : hash {
			   CategoryName,
			   Description,
			   Picture
			}
		}		
	}
	
	relational schema relDB : myRelDB {
		
		table Products {
			columns{
			   ProductID,
			   ProductName,
			   QuantityPerUnit,
			   UnitPrice,
			   UnitsInStock,
			   UnitsOnOrder,
			   ReorderLevel,
			   Discontinued,
			   SupplierRef,
			   CategoryRef
			}
			references{
				supply : SupplierRef -> kvDB.Suppliers.SupplierID
				isCategory : CategoryRef -> kvDB.categoriesKV.catid
			}
		}
		
		
		table Employees {
			columns{
			   EmployeeID,
			   LastName,
			   FirstName,
			   Title,
			   TitleOfCourtesy,
			   BirthDate,
			   HireDate,
			   HomePhone,
			   Extension,
			   Photo,
			   Notes,
			   PhotoPath,
			   Salary,
			   Address,
			   City,
			   Region,
			   PostalCode,
			   Country,
			   ReportsTo
			}
			references{
				manager : ReportsTo -> EmployeeID
			}
			
		}
			
		table Shippers {
			columns{
			   ShipperID,
			   CompanyName,
			   Phone,
		           test: [X]" "[Y]
			}
		}
	}
}

mapping rules {
	csmodel.Orders(id, Freight, OrderDate,RequiredDate, ShipAddress, ShipCity, ShipCountry, ShipName, ShippedDate, ShipPostalCode, ShipRegion) -> mongoDB.Orders(OrderID,Freight,OrderDate,RequiredDate,ShipAddress,ShipCity, ShipCountry, ShipName, ShippedDate, ShipPostalCode, ShipRegion),
	csmodel.Products( productId,ProductName) -> mongoDB.Orders.products( ProductID,ProductName),
	csmodel.Products( productId,Discontinued,ProductName,QuantityPerUnit,ReorderLevel,UnitPrice,UnitsInStock,UnitsOnOrder) -> relDB.Products( ProductID,Discontinued,ProductName,QuantityPerUnit,ReorderLevel,UnitPrice,UnitsInStock,UnitsOnOrder),
	csmodel.Suppliers( supplierId,Address,City,CompanyName,ContactName,ContactTitle,Country,Fax,HomePage,Phone,PostalCode,Region) -> kvDB.Suppliers( SupplierID,Address,City,CompanyName,ContactName,ContactTitle,Country,Fax,HomePage,Phone,PostalCode,Region), 
	csmodel.Customers(customerID,ContactName) -> mongoDB.Orders.customer(CustomerID, ContactName),
	csmodel.Categories( categoryID,CategoryName,Description,Picture) -> kvDB.categoriesKV( catid,CategoryName,Description,Picture),
	csmodel.Shippers( shipperID,CompanyName,Phone) -> relDB.Shippers(ShipperID,CompanyName,Phone),
	csmodel.Employees( employeeID,Address,BirthDate,City,Country,Extension,FirstName,LastName,HireDate,HomePhone,Notes,Photo,PhotoPath,PostalCode,Region,Salary,Title,TitleOfCourtesy)
		->
		relDB.Employees( EmployeeID,Address,BirthDate,City,Country,Extension,FirstName,LastName,HireDate,HomePhone,Notes,Photo,PhotoPath,PostalCode,Region,Salary,Title,TitleOfCourtesy),
	csmodel.Employees( employeeID) -> kvDB.EmployeeOrders( emplid),
	csmodel.register.employeeInCharge -> kvDB.EmployeeOrders.seller,
	csmodel.reportsTo.subordonee -> relDB.Employees.manager,  
	csmodel.Region( regionID,RegionDescription) -> mongoDB.Region( RegionID,RegionDescription),
	csmodel.Territories( territoryID,TerritoryDescription) -> mongoDB.Territories( TerritoryID,TerritoryDescription),
	csmodel.supply.suppliedProduct -> relDB.Products.supply,
	csmodel.typeOf.product -> relDB.Products.isCategory,
	csmodel.locatedIn.territories -> mongoDB.Territories.located,
	csmodel.ships.shippedOrder -> mongoDB.Orders.deliver, 
	csmodel.register.processedOrder -> mongoDB.Orders.encoded,
	csmodel.buy.boughtOrder -> mongoDB.Orders.customer(),
	rel : csmodel.composedOf( Discount,Quantity,UnitPrice) -> mongoDB.Orders.products( Discount,Quantity,UnitPrice),
	csmodel.composedOf.order -> mongoDB.Orders.products(),
	csmodel.typeOf.product -> relDB.Products.isCategory,
	csmodel.locatedIn.territories -> mongoDB.Territories.located,
	csmodel.works.employed -> mongoDB.EmployeeTerritories.employee,
	csmodel.works.territories -> mongoDB.EmployeeTerritories.territory
	
}
databases {
	mysql myRelDB {
		dbname : "reldata"
		host : "localhost"
		port : 3399
		login : "root"
		password : "password"
	}
	
	mongodb myMongoDB{
		host : "localhost"
		port : 27777
	}
	
	redis myRedisDB{
		host : "localhost"
		port : 6666
	}
}