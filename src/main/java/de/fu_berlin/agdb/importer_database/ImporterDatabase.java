package de.fu_berlin.agdb.importer_database;

import de.fu_berlin.agdb.importer.tools.ConnectionManager;
import de.fu_berlin.agdb.importer_database.core.DataCollectionTool;
import de.fu_berlin.agdb.importer_database.core.DatabaseSetupTool;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

public class ImporterDatabase {

    private static final Logger logger = LogManager.getLogger(ImporterDatabase.class);

    @Option(name = "-help", aliases = {"--help", "-H"}, usage = "print this help message", help = true)
    private boolean help = false;

    @Option(name = "-h", aliases = {"-host"}, usage = "the database host")
    private String host = "localhost";
    @Option(name = "-p", aliases = {"-port"}, usage = "port for the database")
    private int port = 5432;
    @Option(name = "-d", aliases = {"-db"}, usage = "name of the database")
    private String database = "ems";
    @Option(name = "-u", aliases = {"-user"}, usage = "username for the database")
    private String user = "ems";
    @Option(name = "-x", aliases = {"-pass"}, usage = "password for the database")
    private String password = "ems";
    @Option(name = "-c", aliases = {"-max-connections"}, usage = "maximum number of connections")
    private int maxConnections = 10;
    @Option(name = "-create", usage = "create tables")
    private boolean createTables = false;

    /* currently not working */
    @Option(name = "-insert", usage = "insert metadata")
    private boolean insertMetadata = false;
    @Option(name = "-D", aliases = {"-collect"}, usage = "collect weather data")
    private boolean collectData = false;
    @Option(name = "-DH", aliases = {"-collect-host"}, usage = "host for data collection")
    private String collectDataHost = "10.10.10.50";
    @Option(name = "-DP", aliases = {"-collect-port"}, usage = "port for data collection")
    private int collectDataPort = 9977;

    public static void main(String[] args) {
        ImporterDatabase importer = new ImporterDatabase();

        // Use `null` as option sorter so the options appear in the specified order
        CmdLineParser parser = new CmdLineParser(importer, ParserProperties.defaults().withOptionSorter(null));

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return;
        }

        if (importer.help) {
            parser.printUsage(System.out);
            return;
        }

        importer.run();
    }

    public void run() {
        BasicConfigurator.configure();
        ConnectionManager connectionManager = new ConnectionManager(host + ":" + Integer.toString(port), database, user, password, maxConnections);

        try {
            if (createTables) {
                logger.log(Level.INFO, "Creating tables...");
                DatabaseSetupTool.createMetaDataTable(connectionManager);
                DatabaseSetupTool.createDWDMetaDataTable(connectionManager);
                DatabaseSetupTool.createWeatherDataTable(connectionManager);
            }

            if(insertMetadata){
	            logger.log(Level.INFO, "Inserting DWD Meta Information...");
	            DatabaseSetupTool.insertDWDMetaInformation(connectionManager);
            }

            if (collectData) {
                logger.log(Level.INFO, "Collection data...");
                new DataCollectionTool(collectDataHost, collectDataPort, connectionManager);
            }
        } catch (Exception e) {
            logger.error("An error occured:", e);
        }
    }
}
