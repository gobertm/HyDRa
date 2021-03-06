# HyDRa
HyDRa (Hybrid Data Representation and Access) is a hybrid polystore management framework providing a modeling language and a conceptual API generation tool.

Modeling language provides support for relational database (MySQL, MariaDB), document database (MongoDB), key value stores (Redis), graph database (Neo4j) and column database (Cassandra).

Based on the designed model a data manipulation API can be generated.

[![Usage Youtube Video](https://img.youtube.com/vi/oTTFhHpt9IY/0.jpg)](https://www.youtube.com/watch?v=oTTFhHpt9IY)

### API Operations Supported
Here we list the operations manipulating modeled domain entities objects supported by the generated API.

| Feature | Relational DB | Document DB | Key Value DB | Graph DB | Column DB |
|----|:---:|:---:|:---:|:---:|---|
| `Read ` | 🌕 | 🌕 | 🌔 (only supports string, hash and lists datatypes) | 🌑 | 🌑 |
| `Insert` | 🌕 | 🌕 | 🌕 | 🌑 | 🌑 |
| `Update` | 🌑 | 🌒 (via insert of embedded entities) | 🌑 | 🌑 | 🌑 |
| `Delete` | 🌑 | 🌑 | 🌑 | 🌑 | 🌑 |

## Install

**A video of the installation and usage of HyDRa tools is available [here](https://github.com/gobertm/HyDRa/raw/main/Use-Cases/resources/Video-Installation-Usage.mp4) or on [youtube](https://youtu.be/-Auy5prYMOw)**
[![youtube](https://img.youtube.com/vi/-Auy5prYMOw/0.jpg)](https://youtu.be/-Auy5prYMOw)

-   Make sure Java 14 is installed on your computer
-   Run Eclipse and install the HyDRa plugins
    -   Help > Install New Software > Add > https://staff.info.unamur.be/gobertm/
-   Restart Eclipse. (Note that in some cases the API generation tool requires a second restart of Eclipse)

![eclipse](Use-Cases/resources/eclipse.PNG)

## Usage
-   Create a new Maven project or import a pre-existing project in [Use-Cases](Use-Cases/) folder.
    -   Please make sure Eclipse uses the Java 14 compiler. Properties > Build path > Select Java 14 JRE. 
    And Properties > Java Compiler > Compiler Compliance Level.
-   Design your HyDRa polystore schema. Create New File > *.pml file extension.
-   Generate the conceptual API code by right-clicking on the .pml file > HyDRa API Generation > Generate HyDRa Conceptual API. Note that if no file is generated, it may be necessary to restart Eclipse and to try again.

![Plugin](Use-Cases/resources/ApiPlugin.PNG)

## Troubleshoot
-   When clicking on 'Generate HyDRa Conceptual API generation' nothing happens.
> Try restarting Eclipse.
- My project is full of compilation errors. 
> Make sure the source containing folders are correctly set in project build path. Build Path > Configure Build path > Source . 2 folders should be listed *project*/src/main/java & *project*/src/main/java
- When running my application code using the generated API I get error :
```
Caused by: java.lang.reflect.InaccessibleObjectException: Unable to make private java.nio.DirectByteBuffer(long,int) accessible: module java.base does not "opens java.nio" to unnamed module @59505b48
``` 
This is caused by an incompatible JDK executing the code. Please make sure to set the Java Compiler to JDK 14.
## Use-Cases 

-   [uc1-imdb](Use-Cases/uc1-imdb) is a complete use case illustrating all functionalities and benefits of the HyDRa framework.
-   [uc2-datainconsistency](Use-Cases/uc2-datainconsistency) is a use case focusing on the data inconsistency discovery feature of HyDRa.
-   [uc3-hybridrelation](Use-Cases/uc3-hybridrelation) further illustrates the handling of hybrid data model references.

## Design of polystore

The HyDRa polystore modeling language was written using Xtext, see [Concrete Syntax](be.unamur.polystore/src/be/unamur/polystore/Pml.xtext)
Below we provide an instantiation example of the three main sections of a HyDRa polystore schema:
1.  The Conceptual Schema section represents the application domain model of the polystore;
2.  The Physical Schema section allows the definition of the physical data structures;
3.  The Mapping Rules section expresses the links between conceptual schema elements and physical schema elements.

![Conceptual Schema](Use-Cases/uc1-imdb/src/main/resources/ConceptualSchema.PNG)
![Physical Schema](Use-Cases/uc1-imdb/src/main/resources/PhysicalSchema.PNG)
![Mapping Rules](Use-Cases/uc1-imdb/src/main/resources/MappingRules.PNG) 

## API Generation

The HyDRa framework also includes a conceptual access API generation process.
A HyDRa polystore schema is given as input of this process. The generation itself is implemented using an Acceleo-based tool (see [acceleo sources](be.unamur.polystore.acceleo)) which generates object classes and data manipulation classes. The API generation tool can be launched by right-clicking on a .pml HyDRa polystore schema.

![API](Use-Cases/resources/ApiGeneration.PNG)

## API Usage Code Example

Below you will find code examples of the generated API usage.
The conceptual model of the code below is the following : 
![Imdb](Use-Cases/resources/Imdbmodel.PNG)

### Get all entity type objects 
Generated Methods signatures :

**Dataset<_E_> get_E_List();**

Example :
```
ActorService actorService = new ActorServiceImpl();
Dataset<Actor> actors = actorService.getActorList();
```

### Get entities by an attribute :
Signature :

**Dataset<_E_> get_E_ListBy_ConceptualAttributeName_(_value_);**

Example:
```
MovieService movieService = new MovieServiceImpl();
Dataset<Movie> movie = movieService.getMovieListByPrimaryTitle("The Big Lebowski");
```

### Get entities by a Condition object : 
Signature :

**Dataset<_E_> get_E_List(Condition<_E_Attribute> _cond_);**
Example : 
```
SimpleCondition<MovieAttribute> condition = new SimpleCondition<>(MovieAttribute.primaryTitle, Operator.EQUALS, "Ocean's Eleven");
movieDataset = movieService.getMovieList(condition);
```
or 
```
directorDataset = directorService.getDirectorList(Condition.simple(DirectorAttribute.id, Operator.EQUALS, "nm0304098"));
```

### Get entities given several attribute values, using an 'and' Condition object :
```
AndCondition<DirectorAttribute> directorCondition = Condition.and(
                Condition.simple(DirectorAttribute.lastName,Operator.EQUALS,"Spielberg"),
                Condition.simple(DirectorAttribute.firstName, Operator.EQUALS, "Steven"));
Dataset<Director> directors = directorService.getDirectorList(directorCondition); 
```

### Get entities of a relationship given opposite entity type Condition object or instance :
Those methods are very useful when you want to get entities linked by a relationship given an attribute or an instance of the other entity. 
Such as *Get me the movies directed by this specific director* or *Get all actors that play in this movie* .

Signature :
**Dataset<_E1_> get_E1_ListIn_RelationshipName_(_E1_._RelationshipName_._RoleE1_, Condition<_E2_Attribute> conditionOn_E2_);**
**Dataset<_E1_> get_E1_ListIn_RelationshipName_(__E1_._RelationshipName_._RoleE1_, _E2_ e2object);**

Examples : 
```
// Get By Condition object
Dataset<Movie> movies = movieService.getMovieList(Movie.direct.directed_movie, conditionSpielberg);
Dataset<Actor> actors = actorService.getActorList(Actor.play.character,Condition.simple(MovieAttribute.primaryTitle,Operator.EQUALS,"Tennet"));

// Get by other entity instance
Director spielberg = directors.collectAsList().get(0); // 'directors' list retrieved before
Dataset<Movie> movies = movieService.getMovieList(Movie.direct.directed_movie, spielberg)
```

### Get Relationship type object :
You can also retrieve object representing a Relationship type, containing their respective entity type in getters.
Getters exist using a condition on relationship type attributes (if any) , or condtions on involved Entity types.
Signature : 

**Dataset<_R_> get_R_List();**
**Dataset<_R_> get_R_List(Condition<_E1_Attribute> condE1, Condition<_E2_Attribute> condE2, Condition<_R_Attribute> condR);**
Example : 
```
PlayService playService = new PlayServiceImpl();
Dataset<Play> plays = playService.getPlayList();
```