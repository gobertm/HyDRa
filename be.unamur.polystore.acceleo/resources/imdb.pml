conceptual schema conceptualSchema{

	entity type Actor {
		id : string,
		firstName : string,
		lastName : string,
		yearOfBirth : int,
		yearOfDeath : int
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
		averageRating : float,
		numVotes : int
		identifier{
			id
		}
	}
	   
    relationship type movieDirector{
		directed_movie[0-N]: Movie,
		director[0-N] : Director
	}
	relationship type movieActor{
		character[0-N]: Actor,
		movie[0-N] : Movie
	}    
}
physical schemas { 
	
	document schema actorCollection : mymongo {
		collection actorInfo {
			fields {
				id,
				fullname:[firstname]" "[lastname],
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
		
		collection topMovies {
			fields {
				id,
				title,
				rate,
				numberofvotes
			}
		}
	}
	
	key value schema movieRedis : myredis {
		kvpairs movieKV {
			key : "movie:"[id],
			value : attr hash{
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
			}
		}
	}
}

mapping rules{
	conceptualSchema.Actor(id,firstName,lastName,yearOfBirth,yearOfDeath) -> actorCollection.actorInfo(id,firstname,lastname,birthyear,deathyear),
	conceptualSchema.movieActor.character-> actorCollection.actorInfo.movies(),
	conceptualSchema.Director(id,firstName,lastName, yearOfBirth,yearOfDeath) -> myRelSchema.directorTable(id,firstname,lastname,birth,death),
	conceptualSchema.movieDirector.director -> myRelSchema.directed.directed_by,
	conceptualSchema.movieDirector.directed_movie -> myRelSchema.directed.has_directed,
	conceptualSchema.Movie(id, primaryTitle) -> actorCollection.actorInfo.movies(id,title),
	conceptualSchema.Movie(averageRating,numVotes) -> actorCollection.actorInfo.movies.rating(rate,numberofvotes),
	conceptualSchema.Movie(id,primaryTitle,averageRating,numVotes) -(averageRating > 9)-> actorCollection.topMovies(id,title,rate,numberofvotes),
	conceptualSchema.Movie(id) -> movieRedis.movieKV(id),
	conceptualSchema.Movie(primaryTitle,originalTitle,isAdult,startYear,runtimeMinutes) ->movieRedis.movieKV.attr(title,originalTitle,isAdult,startYear,runtimeMinutes) 
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