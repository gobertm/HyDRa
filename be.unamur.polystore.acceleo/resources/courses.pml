conceptual schema noe {
	// Based on leaves of generated xsd schemas. + complex object with xml attribute
	// Multi valued complex type are transformed into entity types.
	entity type Course { 
		// presentation
		id : int,
		full_code : string,
		academic_year : int,
		partim_title : string,  
		title : string,
		goals : string,
		objectives : string,
		// presentation - introduction - content
		content : string, // complex with attribute
		course_code : string,
		absolute_credits : string,
		organizer : string,
//		partim_title : string,
		organized : string,
		periodicity : string,
		sustainable : string,
//		title : string,
		theory_q1 : float,
		theory_q2 : float,
		exercises : string, // example of complex object with attribute
		exercises_q1 : float,
		exercises_q2 : float,
		table1 : string,
		// presentation - introduction 
//		exercises : string,
//		exercises_q1 : float,
//		exercises_q2 : float,
		disciplines : string,
		// presenation - pedagogy
		teaching : string,
		assesment : string,
		readings : string,
		// presentation - practical_info		
		language : string,
		location : string,
		contact : string,
		name : string,
		street : string,
		street2 : string,
		zip : int,
		city : string,
		phone : string,
		fax : string,
		email : string,
		cycle : string,
		level : string
		
		identifier{
			id
		}
	}
	
	entity type Teacher {
		last_name : string,
		first_name : string,
		hours : float
	}
	
	entity type Allocation {
		program_code : string,
		program_name : string,
		subjects_group : string,
		credits : float,
		level : string
	}
	
	entity type Discipline {
		discipline : string
	}
	
	entity type PreRequisite {
		name : string
	}
	
	entity type CoRequisite {
		name : string
	}
	
	entity type Item {
		item : string
	}
	
	entity type Content {
		content : string,
		course_code : string,
		absolute_credits : string,
		organizer : string,
		partim_title : string,
		organized: string,
		periodicity : string,
		sustainable : string,
		title : string,
		theory_q1 : float,
		theory_q2 : float,
		exercises : string,
		exercises_q1 : float,
		exercises_q2 : float
	}
	
	
	relationship type given_by {
		course[0-N] : Course,
		professor[0-N]: Teacher 
	}
	
	relationship type depend_on {
		course [0-N] : Course,
		allocation [0-N] : Allocation
	}
	
	relationship type courseDiscipline {
		course [0-N] : Course,
		discipline [1] : Discipline
	}
	
	relationship type prerequisites {
		course [0-N] : Course,
		prerequisite [1] : PreRequisite
	}
	
	relationship type corequisites {
		course[0-N] : Course,
		corequisite [1] : CoRequisite
	}
	
	relationship type pre_items {
		prereq [0-N] : PreRequisite,
		items [1] : Item
	}
	
	relationship type co_items {
		coreq [0-N] : CoRequisite, 
		items [1] : Item
	}
	
	relationship type content_given_by {
		content_course[0-N] : Content,
		professor[0-N]: Teacher 
	}
	
	relationship type content_depend_on {
		content [0-N] : Content,
		allocation [0-N] : Allocation
	}
	
}

physical schemas {
	
	relational schema noe_course : mydb {
		table noe_edu_course_description {
			columns {
				title,
				prerequisites,
				assessment,
				readings,
				content,
				goals,
//				table,
				exercises,
				teaching,
				decree_goals,
				objectives,
				course_orga_id
			}
//			Ne mappe à rien pour l'instant car aucun role ne réprésente cette ref'
//			references {
//				noe_edu_course_description_course_orga_id_fkey : course_orga_id -> noe_edu_course_organization.id
//			}
		}
		
		table noe_edu_course_organization {
			columns {
				id,
				academic_year,
				course_id,
				hours_exercises_q1,
				hours_exercises_q2,
				hours_theory_q1,
				hours_theory_q2, 
				credits, 
				full_code
			}
		}
	}
	
}
mapping rules {
	noe.Course(id, title,assesment,readings,content,goals,exercises,teaching,objectives) -> noe_course.noe_edu_course_description(course_orga_id,title,assessment,readings,content,goals,exercises,teaching,objectives),
	noe.Course(id, academic_year, full_code,exercises_q1,exercises_q2, theory_q1, theory_q2) -> noe_course.noe_edu_course_organization(id, academic_year,full_code, hours_exercises_q1, hours_exercises_q2, hours_theory_q1, hours_theory_q2)
}

databases {
	mariadb mydb {
		dbname : "mydb"
		host : "localhost"
		login : "root"
		password : "password"
		port : 3307
	}
}