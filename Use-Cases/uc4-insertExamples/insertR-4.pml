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
		numVotes : int,
		dummy : string
		identifier{
			id
		}
	}
	
	entity type Review {
		id : string,
		content : string,
		note : int
		identifier {
			id
		}
	}
	
	relationship type movieReview{
		r_reviewed_movie[0-1]: Movie,
		r_review[1] : Review 
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
		
		collection movieCol {
			fields {
				idmovie,
				title,
				reviewid,
				actors [0-N]{
					actorid,
					name
				}
			}
			references{
				selectedReview : reviewid -> myRelSchema.reviewTable.id
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
				movie_info : movie_id -> IMDB_Mongo.movieCol.idmovie
			}
		}
		
		table reviewTable{
			columns{
				id, 
				content,
				movieid
			}
			references{
				onMovie : movieid -> IMDB_Mongo.movieCol.idmovie
			}
		}
	}
}

mapping rules{
	conceptualSchema.Actor(id,fullName,yearOfBirth,yearOfDeath) -> IMDB_Mongo.actorCollection(id,fullname,birthyear,deathyear),
	conceptualSchema.movieActor.character-> IMDB_Mongo.actorCollection.movies(),
	conceptualSchema.Director(id,firstName,lastName, yearOfBirth,yearOfDeath) -> myRelSchema.directorTable(id,firstname,lastname,birth,death),
	conceptualSchema.movieDirector.director -> myRelSchema.directed.directed_by,
	conceptualSchema.movieDirector.directed_movie -> myRelSchema.directed.movie_info,
	conceptualSchema.Movie(averageRating,numVotes) -> IMDB_Mongo.actorCollection.movies.rating(rate,numberofvotes),
	conceptualSchema.Movie(id, primaryTitle) -> IMDB_Mongo.actorCollection.movies(id,title),
	conceptualSchema.Movie(id,primaryTitle) -> IMDB_Mongo.movieCol(idmovie, title),
	conceptualSchema.movieActor.movie -> IMDB_Mongo.movieCol.actors(),
	conceptualSchema.Actor(id,fullName) -> IMDB_Mongo.movieCol.actors(actorid,name),
	conceptualSchema.Review(id,content) -> myRelSchema.reviewTable(id,content),
	conceptualSchema.movieReview.r_reviewed_movie -> IMDB_Mongo.movieCol.selectedReview,
	conceptualSchema.movieReview.r_review -> myRelSchema.reviewTable.onMovie
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
		port: 27100
	}
	
}