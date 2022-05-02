conceptual schema conceptualSchema{

	entity type Actor {
		id : string,
		fullName : string,
		yearOfBirth : string,
		yearOfDeath : string
		identifier {
			id
		}
	}
	
	entity type Director {
		id : string,
		firstName : string,
		lastName : string,
		yearOfBirth : int,
		yearOfDeath : int
		identifier {
			id
		}
	}
	
	entity type Movie {
		id : string,
		primaryTitle : string,
		originalTitle : string,
		isAdult : bool,
		startYear : int,
		runtimeMinutes: int,
		averageRating : string,
		numVotes : int
		identifier{
			id
		}
	}
	   
    relationship type direct {
		directed_movie[0-N]: Movie,
		director[0-N] : Director
	}
	relationship type play{
		character[0-N]: Actor,
		movie[0-N] : Movie
	}    
}
physical schemas { 
	
	document schema IMDB_Mongo : mymongo {
		collection actorCollection {
			fields {
				id,
				fullname,
				birthyear,
				deathyear,
				movies[0-N]{
					id,
					title,
					rating[1]{
						rate: [rate] "/10",
						numberofvotes
					}
				}
			}
		}
	}
	
	key value schema movieRedis : myredis {
		kvpairs movieKV {
			key : "movie:"[id],
			value : hash{
				title,
				originalTitle,
				isAdult,
				startYear,
				runtimeMinutes
			}
		}
	}
	
	relational schema myRelSchema : mydb {
		table directorTable{
			columns{
				id,
				fullname:[firstname]" "[lastname],
				birth,
				death
			}
		}
		
		
		table directed {
			columns{
				director_id,
				movie_id
			}
			references {
				directed_by : director_id -> directorTable.id
				has_directed : movie_id -> movieRedis.movieKV.id
				movie_info : movie_id -> IMDB_Mongo.actorCollection.movies.id
			}
		}
	}
}

mapping rules{
	conceptualSchema.Actor(id,fullName,yearOfBirth,yearOfDeath) -> IMDB_Mongo.actorCollection(id,fullname,birthyear,deathyear),
	conceptualSchema.play.character-> IMDB_Mongo.actorCollection.movies(),
	conceptualSchema.Director(id,firstName,lastName, yearOfBirth,yearOfDeath) -> myRelSchema.directorTable(id,firstname,lastname,birth,death),
	conceptualSchema.direct.director -> myRelSchema.directed.directed_by,
	conceptualSchema.direct.directed_movie -> myRelSchema.directed.has_directed,
	conceptualSchema.direct.directed_movie -> myRelSchema.directed.movie_info,
	conceptualSchema.Movie(id) -> movieRedis.movieKV(id),
	conceptualSchema.Movie(primaryTitle,originalTitle,isAdult,startYear,runtimeMinutes) ->movieRedis.movieKV(title,originalTitle,isAdult,startYear,runtimeMinutes), 
	conceptualSchema.Movie(averageRating,numVotes) -> IMDB_Mongo.actorCollection.movies.rating(rate,numberofvotes),
	conceptualSchema.Movie(id, primaryTitle) -> IMDB_Mongo.actorCollection.movies(id,title)
}

databases {
	
	mariadb mydb {
		host: "localhost"
		port: 3307
		dbname : "mydb"
		password : "password"
		login : "root"
	}
	
	redis myredis {
		host:"localhost"
		port:6379
	}
	
	mongodb mymongo{
		host : "localhost"
		port: 27100
	}
	
}