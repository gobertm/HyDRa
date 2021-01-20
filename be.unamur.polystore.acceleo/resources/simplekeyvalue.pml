conceptual schema conceptualSchema{
		
	entity type Professor {
		Id : int,
		Name : string
	}
	entity type Project {
		Id : int,
		Description : string,
		Name : string
	}
}

physical schemas { 
	key value schema KVResearchProjectsSchema : myredis{
		kvpairs KVProfName {
			key:"PROFESSOR:"[profID]":NAME",
			value:name
		}	
	}
}

mapping rules{
	conceptualSchema.Professor(Id,Name) -> KVResearchProjectsSchema.KVProfName(profID,name)
}

databases {
	redis myredis{
		host:"localhost"
		port:6363
	}
}


