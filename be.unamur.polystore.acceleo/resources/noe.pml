conceptual schema noe {
	entity type CourseDescription { 
		// presentation
		id : int,
		academic_year : int,
		lang:string,
		parent_id : int,
		partim_title : string,
		content : string,  
		title : string,
		goals : string,
		objectives : string,
		course_code : string,
		organizer : string,
		organized : string,
		periodicity : string,
		sustainable : string,
		table1 : string,
		disciplines : string,
		prerequisites: string,
		exercises : string,
		teaching : string,
		assesment : string,
		readings : string,
		cycle : string,
		level : string
		
		identifier{
			id
		}
	}
	
	entity type CourseOrga{
		id: int, 
		absolute_credits : string,
		partim_name : string,
		parent_id : int,
		lang : string,
		academic_year : int,
		cycle_id : int,
		course_code : string,
		exercises_q1 : float,
		exercises_q2 : float,
		theory_q1 : float,
		theory_q2 : float
	}
	
	entity type Program {
		id: int,
		name : string,
		short_name : string,
		group_refer : string
		identifier {id}
	}
	
	entity type ProgramOrga{
		id : int,
		academic_year:int
		identifier{id}
	}
	
	entity type Allocation {
		id : int,
		credits : int,
		required : bool,
		not_to_publish : bool
		identifier{id}
	}
	
	relationship type allocationProg{
		allocation[1]: Allocation,
		program[0-N] : ProgramOrga
	}
	
	relationship type allocationCourse{
		allocation[1] : Allocation,
		course[0-N] : CourseOrga
	}
	
	relationship type organization{
		program[0-N] : Program,
		organization[1] : ProgramOrga
	}
	
	relationship type courseOrgaRel {
		course[1] : CourseDescription,
		orga[0-N] : CourseOrga
	}
}
physical schemas {
	
	relational schema noe_course : mydb {
		table noe_edu_course_description {
			columns {
				id,
				title,
				prerequisites,
				assessment,
				readings,
				content,
				goals,
				table1,
				exercises,
				teaching,
				decree_goals,
				objectives,
				course_orga_id
			}
			references {
				fk_course_orga_id : course_orga_id -> noe_edu_course_organization.id
			}
		}
		
		table noe_edu_course_organization {
			columns {
				id,
				academic_year,
				lang,
				course_id,
				partim_name,
				cycle_id,
				parent_id,
				hours_exercises_q1,
				hours_exercises_q2,
				hours_theory_q1,
				hours_theory_q2, 
				credits, 
				full_code
			}
		}
		
		table noe_edu_program {
			columns {
				id,
				name,
				short_name,
				group_refer				
			}
		}
		
		table noe_edu_program_organization {
			columns{
				id,
				program_id,
				academic_year,
				additional_program_id
			}
			
			references{
				fk_program : program_id -> noe_edu_program.id
				fk_add_program : additional_program_id -> noe_edu_program.id
			}
		}
		
		table noe_edu_allocation {
			columns {
				id,
				required,
				not_to_publish,
				credits,
				program_orga_id,
				course_orga_id
			}
			references {
				fk_course_orga : course_orga_id -> noe_edu_course_organization.id
				fk_program_orga : program_orga_id -> noe_edu_program_organization.id
			}
		}
	}
	
}
mapping rules {
	noe.CourseDescription(id, title, prerequisites, assesment,readings,content,goals,exercises,teaching,objectives) -> noe_course.noe_edu_course_description(id,title, prerequisites, assessment,readings,content,goals,exercises,teaching,objectives),
	noe.CourseOrga(id, cycle_id, partim_name,parent_id, absolute_credits, academic_year,lang, course_code,exercises_q1,exercises_q2, theory_q1, theory_q2) -> noe_course.noe_edu_course_organization(id, cycle_id, partim_name,parent_id, credits, academic_year, lang, full_code, hours_exercises_q1, hours_exercises_q2, hours_theory_q1, hours_theory_q2),
	noe.courseOrgaRel.course -> noe_course.noe_edu_course_description.fk_course_orga_id,
	noe.Program(id,name, short_name, group_refer) -> noe_course.noe_edu_program(id,name,short_name, group_refer),
	noe.ProgramOrga(id,academic_year) -> noe_course.noe_edu_program_organization(id,academic_year),
	noe.organization.organization -> noe_course.noe_edu_program_organization.fk_program,
	noe.organization.organization -> noe_course.noe_edu_program_organization.fk_add_program,
	noe.Allocation(id,credits,required, not_to_publish) -> noe_course.noe_edu_allocation(id,credits,required, not_to_publish),
	noe.allocationCourse.allocation -> noe_course.noe_edu_allocation.fk_course_orga,
	noe.allocationProg.allocation -> noe_course.noe_edu_allocation.fk_program_orga
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