conceptual schema conceptualSchema{

	entity type Actor {
		id : int,
		firstName : string,
		lastName : string,
		gender : string,
		yearOfBirth : int
		identifier {
			id
		}
	}
	
	entity type Director {
		id : int,
		firstName : string,
		lastName : string
		identifier {
			id
		}
	}
	
	entity type DirectorGenre {
		genre : string,
		probability : float
	}
		
		
	entity type Movie {
		id : int,
		name : string,
        year : int,
        rank : float,
		plot : string,
		genre : string,
		release_year : int,
		rating : float,
		votes : int,
		award_win:int,
		award_nomination:int,
		poster : string,
		imdb_id : string
        identifier {
        	id
        }
    }
    
    entity type MovieGenre {
		genre: string
    }
    
    relationship type MovieDirector{
		produced[0-N]: Movie,
		producing[0-N] : Director
	}
	
	relationship type ActorMovie{
		character[0-N]: Actor,
		movie[0-N] : Movie,
		role: string
	}
	
	relationship type HasMovieGenre{
		genre[1]: MovieGenre,
		classifiedMovie[0-N] : Movie
	}
	
	relationship type HasDirectorGenre{
		genre[1]: DirectorGenre,
		classifiedDirectory[0-N] : Director
	}
    
}
physical schemas { 
	
	relational schema myRelSchema : mydb {
		
		table actors {
			columns {
				id,
				first_name,
				last_name,
				gender
			}
		}
		
		table directors {
			columns {
				id,
				first_name,
				last_name
			}
		}
		
		table directors_genres {
			columns {
				director_id,
				genre,
				prob
			}
			
			references{
				director : myRelSchema.directors_genres.director_id -> myRelSchema.directors.id
			}
		}
		
		table movies {
			columns {
				id,
				name,
				year,
				rank
			}
		}
		
		table movies_directors {
			columns {
				director_id,
				movie_id
			}
			
			references{
				directing : myRelSchema.movies_directors.director_id -> myRelSchema.directors.id
				directed : myRelSchema.movies_directors.movie_id -> myRelSchema.movies.id
			}
		}
		
		table movies_genres {
			columns {
				movie_id,
				genre
			}
			
			references{
				movie : myRelSchema.movies_genres.movie_id -> myRelSchema.movies.id
			}
		}
		
		table roles {
			columns {
				actor_id,
				movie_id,
				role
			}
			
			references{
				character : myRelSchema.roles.actor_id -> myRelSchema.actors.id
				movie : myRelSchema.roles.movie_id -> myRelSchema.movies.id
			}
		}
	}
	
	key value schema MovieDBRedis : myredis{
		kvpairs moviesKV {
			key : "movie:"[id],
			value : attr hash{
				title,
				plot,
				genre,
				release_year,
				rating,
				votes,
				poster,
				ibmdb_id
			}
		}
		
		kvpairs actorsKV {
			key : "actor:"[id],
			value : attr hash{
				first_name,
				last_name,
				date_of_birth
			}
		}
		
	}
	
	document schema MflixMongo : mymongo {
		collection movies {
			fields {
				title,
				awards [1]{
					wins,
					nominations,
					description
				}
			}
		}
	}
	
	
	
}

mapping rules{
	// MySQL data mapping
	conceptualSchema.Movie(id,name,year,rank) -> myRelSchema.movies(id,name,year,rank),
	conceptualSchema.Actor(id,firstName,lastName,gender) -> myRelSchema.actors(id,first_name,last_name,gender),
	conceptualSchema.Director(id,firstName,lastName) -> myRelSchema.directors(id,first_name,last_name),
	conceptualSchema.DirectorGenre(genre, probability) -> myRelSchema.directors_genres(genre, prob),
	conceptualSchema.HasMovieGenre.genre -> myRelSchema.movies_genres.movie,
	conceptualSchema.HasDirectorGenre.genre -> myRelSchema.directors_genres.director,
	conceptualSchema.MovieGenre(genre) -> myRelSchema.movies_genres(genre),
	// MongoDB Mflix dataset
	conceptualSchema.Movie(name) -> MflixMongo.movies(title),
	conceptualSchema.Movie(award_nomination, award_win) -> MflixMongo.movies.awards(nominations,wins),
	//Redis data mapping
	conceptualSchema.Movie(id) -> MovieDBRedis.moviesKV(id),
	conceptualSchema.Movie(name, plot,genre, release_year, rating, votes, poster, imdb_id) -> MovieDBRedis.moviesKV.attr(title,plot,genre,release_year,rating,votes,poster, ibmdb_id),
	conceptualSchema.Actor(id) -> MovieDBRedis.actorsKV(id),
	conceptualSchema.Actor(firstName,lastName, yearOfBirth) -> MovieDBRedis.actorsKV.attr(first_name,last_name,date_of_birth)
}

databases {
	
	mariadb mydb {
		host: "localhost"
		port: 3307
		dbname : "mydb"
		password : "password"
		login : "root"
	}
	
	mongodb mymongo{
		host : "localhost"
		port: 27000
	}

	redis myredis {
		host:"localhost"
		port:6379
	}
	
	
}