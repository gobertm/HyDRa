# Developers Guide 
This guide is destined to developpers who cants to extend the tool and release a new version

## Prerequisite 
- Eclipse IDE Java Developpers (2021-03)
- Java JDK 14
- Acceleo and Acceleo SDK (3.7.11)
- XText SDK (2.25)

## Development
- Clone project
- Import project in Eclipse as maven project, be.unamur.polystore .ide .tests .ui .ui.tests as xtext project.
- For **HyDRa modeling language development**, edit the Pml.xtext file
- When finished right click on this file Run As -> Generate Xtext artifacts
- For **HyDRa API Generator** development. Edit the .mtl files be.unamur.polystore.acceleo

## Release
-   Right click on language project -> New Project -> Feature Project
-   Select Language project, ui and ide project -> Finish
-   Right click on acceleo project. -> Acceleo -> Create UI Launcher 
-   Create feature of acceleo and ui project. 
-   For the acceleo context menu to work correctly : Make sure in Generate.java in be.unamur.polystore.acceleo.main  MODULE_FILE_NAME starts with "/bin" 
-   Create a new Update Site project. New Project -> Update Site project
-   Add previously created features to the update site project. Add Feature in site.xml
-   Build all. Features and Plugins folder should appear containing plugins jars.
Build of plugin is complete.

### Install 
- Install New Software in Eclipse
- Add site -> Local -> point to local updatesite folder containing produced jars.
