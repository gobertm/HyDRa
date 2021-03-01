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
		endYear : int,
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
	
	document schema directorCollection : mymongo {
		collection directorInfo {
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
			key : "movie"[id],
			value : attr hash{
				title,
				originalTitle,
				isAdult,
				startYear,
				endYear,
				runtimeMinutes
			}
		}
	}
	
	relational schema myRelSchema : mydb {
		table actorTable{
			columns{
				id,
				firstname,
				lastname,
				birth,
				death
			}
		}
		
		
		table role {
			columns{
				actor_id,
				movie_id
			}
			references {
				played_by : actor_id -> actorTable.id
				plays_in : movie_id -> movieRedis.movieKV.id
			}
		}
	}
}

mapping rules{
	conceptualSchema.Actor(id,firstName,firstName,yearOfBirth,yearOfDeath) -> myRelSchema.actorTable(id,firstname,lastname,birth,death),
	conceptualSchema.movieActor.character-> myRelSchema.role.plays_in,
	conceptualSchema.movieActor.movie -> myRelSchema.role.played_by,
	conceptualSchema.Director(id,firstName,lastName, yearOfBirth,yearOfDeath) -> directorCollection.directorInfo(id,firstname,lastname,birthyear,deathyear),
	conceptualSchema.movieDirector.director -> directorCollection.directorInfo.movies(),
	conceptualSchema.Movie(id, primaryTitle) -> directorCollection.directorInfo.movies(id,title),
	conceptualSchema.Movie(averageRating,numVotes) -> directorCollection.directorInfo.movies.rating(rate,numberofvotes),
	conceptualSchema.Movie(id,primaryTitle,averageRating,numVotes) -(averageRating > 9)-> directorCollection.topMovies(id,title,rate,numberofvotes),
	conceptualSchema.Movie(id) -> movieRedis.movieKV(id),
	conceptualSchema.Movie(primaryTitle,originalTitle,isAdult,startYear,endYear,runtimeMinutes) ->movieRedis.movieKV.attr(title,originalTitle,isAdult,startYear,endYear,runtimeMinutes) 
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
		port: 27000
	}
	
}