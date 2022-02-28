conceptual schema conceptualSchema{
		
	entity type User {
		userName : string,
		firstName : string,
        lastName : string,
        email:string,
        creationDate:datetime
        identifier {
        	userName
        }
    }
    
    entity type Video {
    	id: string,
    	description: string,
    	name: string
    	identifier {
    		id
    	}
    }
    
    entity type Category {
    	id: int,
    	name: string,
    	description: string,
    	picture: string
    	identifier {
    		id
    	}
    }
    
    relationship type Comment {
		commenter[0-N]: User,
		video[0-N] : Video,
		comment: string,
		dateCom: datetime
	}
}
physical schemas { 
	relational schema relSchema: mydb {
		table Categories {
			columns {
				CategoryID,
				CategoryName,
				Description,
				Picture
			}
		}
	}
	
	column schema myKeyspace: videodb {
		table users {
			columns {
				username,
				firstname,
				lastname,
				email,
				created_date
			}
		}
		
		table videos {
			columns {
				videoid,
				description,
				videoname
			}
		}
		
		table comments_by_video {
			columns {
				video_id,
				user_name,
				comment,
				comment_ts
			}
			
			references {
				video: video_id -> myKeyspace.videos.videoid
				commenter: user_name -> myKeyspace.users.username
			}
		}
	}
	
	
}

mapping rules{
	conceptualSchema.User(userName,firstName, lastName, email, creationDate) -> myKeyspace.users(username, firstname,lastname, email, created_date),
	conceptualSchema.Video(id, description, name) ->  myKeyspace.videos(videoid, description, videoname),
	conceptualSchema.Comment.commenter ->  myKeyspace.comments_by_video.commenter,
	conceptualSchema.Comment.video ->  myKeyspace.comments_by_video.video,
	rel: conceptualSchema.Comment(comment, dateCom) ->  myKeyspace.comments_by_video(comment, comment_ts),
	conceptualSchema.Category(id, name, description, picture) -> relSchema.Categories(CategoryID, CategoryName, Description, Picture)
}

databases {
	cassandra videodb{
		host:"localhost"
		port: 9042
		dbname: "videodb"
	}
	
	mariadb mydb {
		host: "localhost"
		port: 3306
		dbname : "northwind"
		password : "password"
		login : "root"
	}
	
}