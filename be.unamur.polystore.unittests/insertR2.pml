conceptual schema insertR2{

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
		collection userCol{
			fields {
				iduser,
				account[1]{
					idaccount,
					email
				}
			}
		}
		
		collection actorCol {
			fields{ 
				idactor,
				movies[0-N]{
					idmovie,
					reviews[0-N]{
						idreview,
						commenter[1]{
							iduser
						}
					}
				}
			}
		}		
	}
	
	relational schema myRelSchema : mydb{
		table directorTable {
			columns{
				id,
				fullname : [firstname]" "[lastname]
			}
		}
		
		table movieTable {
			columns{
				id,
				title,
				director_id
			}
			references{
				directorRef : director_id -> directorTable.id
			}
		}
	}
	
}
	

mapping rules{
	insertR2.User(id) -> IMDB_Mongo.userCol(iduser),
	insertR2.Account(id, email) -> IMDB_Mongo.userCol.account(idaccount, email),
	insertR2.userAccount.user -> IMDB_Mongo.userCol.account(),
	
	insertR2.Actor(id) -> IMDB_Mongo.actorCol(idactor),
	insertR2.Movie(id) -> IMDB_Mongo.actorCol.movies(idmovie),
	insertR2.Review(id) -> IMDB_Mongo.actorCol.movies.reviews(idreview),
	insertR2.User(id) -> IMDB_Mongo.actorCol.movies.reviews.commenter(iduser),
	insertR2.movieActor.character -> IMDB_Mongo.actorCol.movies(),
	insertR2.movieReview.r_reviewed_movie -> IMDB_Mongo.actorCol.movies.reviews(),
	insertR2.reviewUser.r_review1 -> IMDB_Mongo.actorCol.movies.reviews.commenter(),
	
	insertR2.Director(id,firstName,lastName) -> myRelSchema.directorTable(id,firstname,lastname),
	insertR2.Movie(id,primaryTitle) -> myRelSchema.movieTable(id,title),
	insertR2.movieDirector.directed_movie -> myRelSchema.movieTable.directorRef
}

databases {
	
	mariadb mydb {
		host: "mydb"
		port: 3306
		dbname : "mydb"
		password : "password"
		login : "root"
	}
	
	mongodb mymongo{
		host : "mymongo"
		port: 27017
	}
	
}