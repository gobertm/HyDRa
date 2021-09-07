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
	
	entity type User {
		id : string,
		username : string,
		city : string
		identifier{
			id
		}
	}
	   
    relationship type movieDirector{
		directed_movie[1-N]: Movie,
		director[0-N] : Director
	}
	relationship type movieActor{
		character[0-N]: Actor,
		movie[1-N] : Movie
	}
	relationship type movieReview{
		r_reviewed_movie[0-N]: Movie,
		r_review[1] : Review 
	}
	relationship type reviewUser{
		r_author[0-N]: User,
		r_review1[1] : Review
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
		
		collection reviewCol {
			fields{
				idreview,
				content,
				note,
				author[1]{
					authorid,
					username,
					city
				},
				movie[1]{
					movieid,
					title,
					avgrating,
					actors[1-N]{ // Only for the purpose of complex embedded structure
						id,
						name
					}
				} 
			}
		}
		
		collection movieCol {
			fields {
				idmovie,
				actors [1-N]{
					actorid,
					name
				},
				directors [1-N]{
					 directorid,
					 firstname,
					 lastname
				}
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
				movie_info : movie_id -> IMDB_Mongo.actorCollection.movies.id
			}
		}
		
		table reviewTable{
			columns{
				id, 
				content
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
	conceptualSchema.Movie(id,primaryTitle, averageRating) -> IMDB_Mongo.reviewCol.movie(movieid,title,avgrating),
	conceptualSchema.Review(id,content,note) -> IMDB_Mongo.reviewCol(idreview,content,note),
	conceptualSchema.User(id,username,city) -> IMDB_Mongo.reviewCol.author(authorid,username,city),
	conceptualSchema.reviewUser.r_review1 -> IMDB_Mongo.reviewCol.author(),
	conceptualSchema.movieReview.r_review -> IMDB_Mongo.reviewCol.movie(),
	// Complex embedded structure
	conceptualSchema.movieActor.movie -> IMDB_Mongo.reviewCol.movie.actors(),
	conceptualSchema.Actor(id,fullName) -> IMDB_Mongo.reviewCol.movie.actors(id,name),
	// Standalone structure
	conceptualSchema.Review(id, content) -> myRelSchema.reviewTable(id,content),
	// Descending structure 
	conceptualSchema.Movie(id) -> IMDB_Mongo.movieCol(idmovie),
	conceptualSchema.movieActor.movie -> IMDB_Mongo.movieCol.actors(),
	conceptualSchema.movieDirector.directed_movie -> IMDB_Mongo.movieCol.directors(),
	conceptualSchema.Director(id,firstName,lastName) -> IMDB_Mongo.movieCol.directors(directorid,firstname,lastname),
	conceptualSchema.Actor(id,fullName) -> IMDB_Mongo.movieCol.actors(actorid,name)
	// Ascending structure 
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