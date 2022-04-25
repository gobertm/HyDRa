databases {
	mysql relData {
		dbname : "reldata"
		host : "localhost"
		login : "root"
		password : "password"
		port : 3399
	}
	mongodb myMongoDB {
		dbname : "myMongoDB"
		host : "localhost"
		port : 27777
	}
	redis redisDB {
		host : "localhost"
		port : 6666
	}
}
// Conceptual schema
conceptual schema conceptualSchema {
	//entities
	entity type Shippers { 
		shipperID : int,
		companyName : string, 
		phone : string
		identifier {
			shipperID
		}
	}
	entity type Products {
		productID : int, 
		productName : string, 
		// SupplierRef : int,
		// CategoryRef : int,
		quantityPerUnit : string,
		unitPrice : float,
		unitsInStock : int,
		unitsOnOrder : int,
		reorderLevel : int,
		discontinued : bool
		identifier {
			productID
		}
	}
	entity type Suppliers { // OK
		supplierID : int,
		companyName : string,
		contactName : string, 
		contactTitle : string,
		address : string,
		city : string,
		region : string,
		postalCode : string,
		country : string,
		phone : string,
		fax : string,
		homePage : string
		identifier {
			supplierID
		}
	}
	entity type Customers { // ok
		customerID : string,
		companyName : string, 
		contactName : string, 
		contactTitle : string, 
		address : string,
		city : string,
		region : string, 
		postalCode : string,
		country : string, 
		phone : string, 
		fax : string
		identifier {
			customerID
		}
	}
	entity type Orders { // ok
		orderID : int,
		// CustomerRef : string,
		// EmployeeRef : int,
		orderDate : date,
		requiredDate : date,
		shippedDate : date,
		shipVia : int,
		freight : float,
		shipName : string,
		shipAddress : string,
		shipCity : string,
		shipRegion : string,
		shipPostalCode : string, 
		shipCountry : string
		identifier {
			orderID
		}
	}
	entity type Categories { // ok
		categoryID : int,
		categoryName : string,
		description : string,
		picture : string
		identifier {
			categoryID
		}
	}
	// entity type EmployeeTerritories {  // ok
	// 	EmployeeRef : int,
	// 	TerritoryRef : string
	// }

	entity type Employees { // ok
		employeeID : int,
		firstName : string,
		lastName : string,
		title : string,
		titleOfCourtesy : string,
		birthDate : date,
		hireDate : date,
		address : string, 
		city : string,
		region : string,
		postalCode : string,
		country : string,
		homePhone : string,
		extension : string,
		photo : string,
		notes : string,
		reportsTo : int,
		photoPath : string,
		salary : float
		identifier {
			employeeID
		}
	}
	entity type Region { // ok 
		regionID : int,
		regionDescription : string
		identifier {
			regionID
		}
	}
	entity type Territories { // ok
		territoryID : string,
		territoryDescription : string,
		regionRef : int
		identifier {
			territoryID
		}
	}
	//relationships
	relationship type make_by {
		order[1]: Orders,
		client[0-N]: Customers
	}
	relationship type contains {
		territory[1]: Territories,
		region[1-N]: Region
	}
	relationship type are_in {
		employee[1-N]: Employees,
		territory[1-N]: Territories 
	}
	relationship type report_to {
		lowerEmployee[0-1]: Employees,
		higherEmployee[0-N]: Employees
	}
	relationship type ship_via {
		shipper[1-N]: Shippers,
		order[1]: Orders
	}
	relationship type handle {
		employee[1-N]: Employees,
		order[1]: Orders
	}
	relationship type insert {
		supplier[1-N]: Suppliers,
		product[1]: Products
		
	}
}
//Physical schema
physical schemas {
	key value schema kv : redisDB {
		kvpairs shipperCompanyNamePairs {
			key : "SHIPPERS:"[SHIPPERSid]":COMPANYNAME",
			value : COMPANYNAME
		}
		kvpairs shipperPhonePairs {
			key : "SHIPPERS:"[SHIPPERSid]":PHONE",
			value : PHONE 
		}
		kvpairs stockInfoPairs {
			key : "PRODUCT:"[productid]":STOCKINFO",
			value : hash {
				UnitsInStock,
				UnitsOnOrder
			}
		}
	}
	relational schema relSchema : relData {
		table Territories {
			columns {
				TerritoryID,
				TerritoryDescription ,
				RegionRef 
			}
		}
		table EmployeeTerritories {
			columns {
				EmployeeRef ,
				TerritoryRef 
			}
		}
		table Employees {
			columns {
				EmployeeID ,
				FirstName ,
				LastName ,
				Title ,
				TitleOfCourtesy ,
				BirthDate,
				HireDate,
				Address ,
				City ,
				Region ,
				PostalCode ,
				Country ,
				HomePhone ,
				Extension,
				Photo,
				Notes,
				ReportsTo ,
				PhotoPath,
				Salary
			}
		}

		table Region {
			columns {
				RegionID ,
				RegionDescription
			}
		}

		
		table Shippers {
			columns {
				ShipperID ,
				CompanyName ,
				Phone 
			}
		}

		table Orders {
			columns {
				OrderID ,
				CustomerRef,
				EmployeeRef ,
				OrderDate,
				RequiredDate,
				ShippedDate,
				ShipVia ,
				Freight ,
				ShipName ,
				ShipAddress,
				ShipCity ,
				ShipRegion,
				ShipPostalCode ,
				ShipCountry 
			}
		}

		table Order_Details {
			columns {
				OrderRef ,
				ProductRef ,
				UnitPrice ,
				Quantity ,
				Discount 
			}
		}

		table Customers {
			columns {
				CustomerID,
				CompanyName,
				ContactName ,
				ContactTitle ,
				Address ,
				City ,
				Region ,
				PostalCode ,
				Country ,
				Phone ,
				Fax  
			}
		}

		table Products {
			columns {
				ProductID,
				ProductName,
				SupplierRef,
				CategoryRef,
				QuantityPerUnit,
				UnitPrice,
				UnitsInStock,
				UnitsOnOrder,
				ReorderLevel,
				Discontinued
			}
		}

		table Categories {
			columns {
				CategoryID,
				CategoryName,
				Description,
				Picture
			}
		}


		table Suppliers {
			columns {
				SupplierID,
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


		table ProductsInfo {
			columns {
				ProductID,
				ProductName,
				SupplierRef,
				CategoryRef,
				QuantityPerUnit,
				UnitPrice,
				ReorderLevel,
				Discontinued
			}
		}
	
	}
	document schema mongoSchema : myMongoDB {
		collection Customers {
			fields {
				ID,
				Address,
				City,
				CompanyName,
				ContactName,
				ContactTitle,
				Country,
				Fax,
				Phone,
				PostalCode,
				Region
			}
		}
		collection Employees {
			fields {
				EmployeeID,
				Address,
				BirthDate,
				City,
				Country,
				Extension,
				FirstName,
				HireDate,
				HomePhone,
				LastName,
				Notes,
				Photo,
				PhotoPath,
				PostalCode,
				Region,
				Salary,
				ReportsTo,
				Title,
				TitleOfCourtesy
			}
		}
		collection Orders {
			fields {
				OrderID,
				EmployeeRef,
				Freight,
				OrderDate,
				RequiredDate,
				ShipAddress,
				ShipCity,
				ShipCountry,
				ShipName,
				ShipPostalCode,
				ShipRegion,
				ShipVia,
				ShippedDate
			//	customer[1]{
			//		CustomerID,
			//		ContactName
			//  }
			}
			// references {
			//	empOrder: EmployeeRef -> Employees.EmployeeID
			//}
		}
		collection Suppliers {
			fields {
				SupplierID,
				Address,
				City,
				CompanyName,
				ContactName,
				ContactTitle,
				Country,
				Fax,	
				Phone,
				HomePage,
				PostalCode,
				Region
			}
		}
	}
}
mapping rules {
	//Employee
	conceptualSchema.Employees(employeeID, address, birthDate, city, country, extension, firstname, hireDate, homePhone, lastname, photo, postalCode, region, salary, title, notes, photoPath, titleOfCourtesy) 
	-> mongoSchema.Employees(EmployeeID, Address, BirthDate, City, Country, Extension, FirstName, HireDate, HomePhone, LastName, Photo, PostalCode, Region, Salary, Title, Notes, PhotoPath, TitleOfCourtesy),
	
	//Region
	conceptualSchema.Region(regionID, description) -> mongoSchema.Employees.territories.region(RegionID, RegionDescription),
	
	//Territory
	conceptualSchema.Territories(territoryID, description) -> mongoSchema.Employees.territories(TerritoryID, TerritoryDescription),
	
	//Product
	conceptualSchema.Products(productID, name, supplierRef, categoryRef,quantityPerUnit, unitPrice, reorderLevel, discontinued) -> relSchema.ProductsInfo(ProductID, ProductName, SupplierRef, CategoryRef, QuantityPerUnit, UnitPrice, ReorderLevel, Discontinued),
	conceptualSchema.Products(productID, unitsInStock, unitsOnOrder) -> kv.stockInfoPairs(productid, UnitsInStock, UnitsOnOrder),
	
	//Category
	conceptualSchema.Categories(categoryID, categoryName, description, picture) -> kv.categoryPairs(categoryid, CategoryName, Description, Picture),	
	
	//Shipper
	conceptualSchema.Shippers(shipperID, companyName, phone) -> relSchema.Shippers(ShipperID, CompanyName, Phone),
	
	//Customer
	conceptualSchema.Customers(customerID, city, companyName, contactName, contactTitle, country, fax, phone, postalCode, region, address) -> mongoSchema.Customers(ID, City, CompanyName, ContactName, ContactTitle, Country, Fax, Phone, PostalCode, Region, Address),
	conceptualSchema.Customers(customerID, companyName) -> mongoSchema.Orders.customer(CustomerID, ContactName),
		
	//Order
	conceptualSchema.Orders(orderID, freight, orderDate, requiredDate, shipAddress, shipCity, shipCountry, shipName, shipPostalCode, shipRegion, shippedDate) -> mongoSchema.Orders(OrderID, Freight, OrderDate, RequiredDate, ShipAddress, ShipCity, ShipCountry, ShipName, ShipPostalCode, ShipRegion, ShippedDate),
	
	//Supplier
	conceptualSchema.Suppliers(supplierID, address, city, companyName, contactName, contactTitle, country, fax, homePage, phone, postalCode, region) 
	-> mongoSchema.Suppliers(SupplierID, Address, City, CompanyName, contactName, ContactTitle, Country, Fax, HomePage, Phone, PostalCode, Region)
	//Employee -> Order
	//conceptualSchema.handle.order -> mongoSchema.Orders.empOrder
	
}
