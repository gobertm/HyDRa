# HyDRa
HyDRa (Hybrid Data Representation and Access) is a hybrid polystore management framework providing a modeling language and a conceptual API generation tool.

## Install

-   Make sure to have Java 14 installed
-   Run Eclipse and install HyDRa plugins
    -   Help > Install New Software > Add > https://staff.info.unamur.be/gobertm/
-   Restart Eclipse. (Note that in some cases the API generation need a second Eclipse restart)

## Usage
-   Create New Maven project or import pre existing ones in APIs folder.
    -   Please make sure to use Java 14 compiler. Properties > Build path > Select Java 14 JRE. 
    And Properties > Java Compiler > Compiler Compliance Level.
-   Design HyDRa Polystore schema.
    -   Create an empty pml file and open it with the PML Editor.
        
        or
    -   Put the desired .pml HyDRa model file at the root of the project.
-   Generate API Code by right clicking on a .pml file > HyDRa API Generation > Generate HyDRa Conceptual API. Note that if no files are generated it may be necessary to restart Eclipse.
