package be.unamur;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class MainCLI {
    static final Logger logger = LoggerFactory.getLogger(MainCLI.class);

    private static final String OPT_REDIS_PORT = "redisport";
    private static final String OPT_REDIS_HOST = "redishost";
    private static final String OPT_MONGO_HOST = "mongohost";
    private static final String OPT_MONGO_PORT = "mongoport";
    private static final String OPT_NEO_PORT = "neoport";
    private static final String OPT_NEO_HOST = "neohost";
    private static final String OPT_NEO_USER = "neouser";
    private static final String OPT_NEO_PWD = "neopwd";
    private static final String OPT_NBDATAOBJ = "nbdata";
    private static final Options options = new Options();

    public static void main(String args[]) {
        buildOptions();

        //Default values
        int redisport = 6363;
        int mongoport = 27000;
//        int neoport = 7474;// HTTP
        int neoport = 7687;// Bolt
        int nbdataobj = 100;
        String redishost = "localhost";
        String mongohost = "localhost";
        String mongodbname = "mymongo";
        String neohost = "localhost";
        String neouser = "neo4j";
        String neopwd = "myneo4j";


        CommandLineParser cliparser = new DefaultParser();
        CommandLine commandLine;
        try {
            commandLine = cliparser.parse(options, args);
            if (commandLine.hasOption(OPT_REDIS_PORT)) {
                redisport = Integer.parseInt(commandLine.getOptionValue(OPT_REDIS_PORT));
            }
            if (commandLine.hasOption(OPT_REDIS_HOST)) {
                redishost = commandLine.getOptionValue(OPT_REDIS_HOST);
            }if (commandLine.hasOption(OPT_MONGO_HOST)) {
                mongohost = commandLine.getOptionValue(OPT_MONGO_HOST);
            }if (commandLine.hasOption(OPT_MONGO_PORT)) {
                mongoport = Integer.parseInt(commandLine.getOptionValue(OPT_MONGO_PORT));
            }
            if (commandLine.hasOption(OPT_NEO_HOST)) {
                neohost = commandLine.getOptionValue(OPT_NEO_HOST);
            }
            if (commandLine.hasOption(OPT_NEO_USER)) {
                neouser = commandLine.getOptionValue(OPT_NEO_USER);
            }
            if (commandLine.hasOption(OPT_NEO_PWD)) {
                neopwd = commandLine.getOptionValue(OPT_NEO_PWD);
            }
            if (commandLine.hasOption(OPT_NEO_PORT)) {
                neoport = Integer.parseInt(commandLine.getOptionValue(OPT_NEO_PORT));
            }
            if (commandLine.hasOption(OPT_NBDATAOBJ)) {
                nbdataobj = Integer.parseInt(commandLine.getOptionValue(OPT_NBDATAOBJ));
            }
            RedisDataInit redisDataInitialization = new RedisDataInit(redishost, redisport, nbdataobj );
            redisDataInitialization.persistData();

            MongoDataInit mongoDataInit = new MongoDataInit(mongodbname, mongohost, mongoport, nbdataobj);
            mongoDataInit.persistData();

            Neo4jDataInit neo4jDataInit = new Neo4jDataInit(neohost, neoport, neouser, neopwd, CypherExecutorType.JAVADRIVER_EXEC);
            neo4jDataInit.createProducts("http://data.neo4j.com/northwind/products.csv");
            neo4jDataInit.createCategories("http://data.neo4j.com/northwind/categories.csv");
            neo4jDataInit.createSuppliers("http://data.neo4j.com/northwind/suppliers.csv");
            neo4jDataInit.createRelationships();
            neo4jDataInit.close();
        } catch (ParseException exp) {
            logger.error("Parsing failed. ", exp);
        } catch (SQLException e) {
            logger.error("Connnection failed. ", e);
        }

    }

    private static void buildOptions() {
        Option redisportOption = new Option(OPT_REDIS_PORT, true, "port of redis database");
        Option redishostOption = new Option(OPT_REDIS_HOST, true, "host of redis database");
        Option mongoportOption = new Option(OPT_MONGO_PORT, true, "port of mongo database");
        Option mongohostOption = new Option(OPT_MONGO_HOST, true, "host of mongo database");
        Option neohostOption = new Option(OPT_NEO_HOST, true, "host of neo4j database");
        Option neoportOption = new Option(OPT_NEO_PORT, true, "port of neo4j database");
        Option nbDataOption = new Option(OPT_NBDATAOBJ, "number of data objects to persist");
        options.addOption(redisportOption);
        options.addOption(redishostOption);
        options.addOption(mongohostOption);
        options.addOption(mongoportOption);
        options.addOption(neohostOption);
        options.addOption(neoportOption);
        options.addOption(nbDataOption);
    }


}
