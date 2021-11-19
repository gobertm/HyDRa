Installation instruction below or on [home page](https://github.com/gobertm/HyDRa)

# Use Case 5

This use case is available as a video (with audio comments) on [youtube](https://youtu.be/oTTFhHpt9IY)

## **Description**
This use case shows how HyDRa API code can be used in an evolution context.
 - [Test](/src/test/java/Tests.java) class contains functions using HyDRa generated code. This code can access data wether modelA or modelB polystore has been deployed.

 ## ** How-to ** 
 1. Unzip [modelA data](data-deployment/ModelA/data-ModelA.zip) & [modelB data](data-deployment/ModelB/data-ModelB.zip)
 2. Deploy modelA and modelB docker compose files.
    - Run command : 'docker-compose "docker-compose-ModelA.yml" up
    - Run command : 'docker-compose "docker-compose-ModelB.yml" up
3. Data is deploying it may take a while. 
4. Import [UC5](https://github.com/gobertm/HyDRa/tree/main/Use-Cases/uc5-SANER2022) project in Eclipse.
5. Generate HyDRa Conceptual API, on modelA.pml or on modelB.pml 
    - Right click on .pml file > HyDRa API Generation > Generate ...
6. You may now execute [Test](/src/test/java/Tests.java) or write your own code!



## Install

**A video of the installation and usage of HyDRa tools is available [here](https://github.com/gobertm/HyDRa/raw/main/Use-Cases/resources/Video-Installation-Usage.mp4) or on [youtube](https://youtu.be/-Auy5prYMOw)**

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


Please go to homepage for a more detailed [README](https://github.com/gobertm/HyDRa)
