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
	
	entity type Account {
		id: string,
		email : string,
		pass : string
		identifier{
			id
		}
	}
	
	relationship type userAccount {
		user[1] : User,
		account[1] : Account
	}
	   
    relationship type movieDirector{
		directed_movie[1]: Movie,
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
		collection movieCol {
			fields{
				movieid,
				title,
				reviews[0-N]{
					idreview,
					content,
					note,
					author[1]{
						iduser,
						username
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
					iduser,
					username
				},
				idmovie
			}
			references{
				movie : idmovie -> movieCol.movieid
			}
		}
	}
}
	

mapping rules{
	conceptualSchema.Movie(id,primaryTitle) -> IMDB_Mongo.movieCol(movieid,title),
	conceptualSchema.Review(id,content, note) -> IMDB_Mongo.movieCol.reviews(idreview,content,note),
//	conceptualSchema.Review(id,content, note) -(note>4)-> IMDB_Mongo.movieCol.reviews(idreview,content,note), // OR find a way to express more complex conditions (submit_date = top 5 most recent)
	conceptualSchema.Review(id, content, note) -> IMDB_Mongo.reviewCol(idreview,content,note),
	conceptualSchema.User(id,username) -> IMDB_Mongo.movieCol.reviews.author(iduser,username),
	conceptualSchema.User(id,username) -> IMDB_Mongo.reviewCol.author(iduser,username),
	
	conceptualSchema.reviewUser.r_review1 -> IMDB_Mongo.movieCol.reviews.author(),
	conceptualSchema.reviewUser.r_review1 -> IMDB_Mongo.reviewCol.author(),
	conceptualSchema.movieReview.r_reviewed_movie -> IMDB_Mongo.movieCol.reviews(),
	conceptualSchema.movieReview.r_review -> IMDB_Mongo.reviewCol.movie
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