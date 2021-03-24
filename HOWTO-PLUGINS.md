# How to build and install plugins Eclipse 
## for PML Language and Acceleo PML DAO Generator

### PML Language plugin
-   Right click on language project -> New Project -> Feature Project
-   Select Language project and ui project -> Finish
-   Right click on acceleo project. -> Acceleo -> Create UI Launcher 
-   Create feature of acceleo and ui project. 
-   For the acceleo context menu to work correctly, it is necessary to copy .emtl files (from bin directory, as they are compiled version of the .mtl files, each time acceleo code is modified, the files have to be rewritten) and paste them alongside their originating .mtl files . (see https://www.eclipse.org/forums/index.php/t/1078907/) 
-   Create a new Update Site project. New Project -> Update Site project
-   Add previously created features to the update site project. Add Feature in site.xml
-   Build all. Features and Plugins folder should appear containing plugins jars.
Build of plugin is complete.

### Install 
- Install New Software in Eclipse
- Add site -> Local -> point to updatesite folder.