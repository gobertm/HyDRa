conceptual schema conceptualSchema{

	entity type Client {
		id: int,
		name: string
		identifier {
			id
		}
	}
	
	entity type Adresse {
		id: int,
		street: string
	}
	
	entity type Commande {
		id: int,
		dates: string
		identifier {
			id
		}
	}
	
	relationship type Vit {
		resident[1]: Client
		adresse[0-N]: Adresse
	}
	
	relationship type Passe {
		acheteur[0-N]: Client,
		achete[0-N] : Commande
	}
	
	
    
}
physical schemas { 
	
	relational schema myRelSchema : mydb {
	
		table ADRESSE {
			columns {
				id,
				street
			}
			
		}
		
		table CLIENT {
			columns {
				ID1,
				NAME,
				ADRESSE
			}
			
			references {
				vit : ADRESSE -> myRelSchema.ADRESSE.id
			}
		}
		
		table COMMANDE {
			columns {
				ID2,
				DATE
			}
			
		}
		
	}
	
	document schema categorySchema : mymongo {
			collection cli_com {
				fields {
					cli,
					com
				}
				
				references {
					achat_par : cli -> myRelSchema.CLIENT.ID1
					achat_de : com -> myRelSchema.COMMANDE.ID2
				}
			}
		
	}	
	
}

mapping rules{
	conceptualSchema.Client(id, name) -> myRelSchema.CLIENT(ID1, NAME),
	conceptualSchema.Adresse(id, street) -> myRelSchema.ADRESSE(id, street),
	conceptualSchema.Commande(id, dates) -> myRelSchema.COMMANDE(ID2, DATE),
	conceptualSchema.Passe.acheteur -> categorySchema.cli_com.achat_par,
	conceptualSchema.Passe.achete -> categorySchema.cli_com.achat_de,
	conceptualSchema.Vit.resident -> myRelSchema.CLIENT.vit
	
}

databases {
	
	mariadb mydb {
		host: "localhost"
		port: 3306
		dbname : "test"
		password : "admin"
		login : "root"
	}
	
	mongodb mymongo {
		host:"localhost"
		port: 27000
		dbname: "admin"
		login: "admin"
		password: "admin"
	}
	
}