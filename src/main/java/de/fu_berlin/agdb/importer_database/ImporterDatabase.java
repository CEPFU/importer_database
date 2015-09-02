package de.fu_berlin.agdb.importer_database;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import de.fu_berlin.agdb.importer.tools.ConnectionManager;
import de.fu_berlin.agdb.importer_database.core.DataCollectionTool;
import de.fu_berlin.agdb.importer_database.core.DataReplayTool;
import de.fu_berlin.agdb.importer_database.core.DatabaseSetupTool;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ImporterDatabase {

    private static final Logger logger = LogManager.getLogger(ImporterDatabase.class);

    Properties properties;
    
    public static void main(String[] args) throws FileNotFoundException, IOException {
        ImporterDatabase importer = new ImporterDatabase();
        importer.run();
    }
    
    public ImporterDatabase() throws FileNotFoundException, IOException {
    	properties = new Properties();
    	FileReader reader = new FileReader(new File("importer-database.properties"));
		properties.load(reader);
		reader.close();
	}

    public void run() {
        ConnectionManager connectionManager = new ConnectionManager(properties.getProperty("database_host") + ":" + properties.getProperty("database_port"), 
        		properties.getProperty("database"), properties.getProperty("database_user"), 
        		properties.getProperty("database_password"), Integer.valueOf(properties.getProperty("maximum_database_connections")));

        try {
            if (Boolean.valueOf(properties.getProperty("create_database_tables"))) {
                logger.log(Level.INFO, "Creating tables...");
                DatabaseSetupTool.createMetaDataTable(connectionManager);
                DatabaseSetupTool.createDWDMetaDataTable(connectionManager);
                DatabaseSetupTool.createWeatherDataTable(connectionManager);
            }

            if(Boolean.valueOf(properties.getProperty("insert_dwd_metadata"))){
	            logger.log(Level.INFO, "Inserting DWD Meta Information...");
	            DatabaseSetupTool.insertDWDMetaInformation(connectionManager);
            }

            if (Boolean.valueOf(properties.getProperty("collect_data"))) {
                logger.log(Level.INFO, "Collection data...");
                new DataCollectionTool(properties.getProperty("collect_data_host"), 
                		Integer.valueOf(properties.getProperty("collect_data_port")), connectionManager);
            }
            
            if(Boolean.valueOf(properties.getProperty("replay_data"))){
            	logger.log(Level.INFO, "Replaying previously collected data");
            	new DataReplayTool(Integer.valueOf(properties.getProperty("replay_data_port")), 
            			connectionManager, Long.valueOf(properties.getProperty("replay_intervall")));
            }
        } catch (Exception e) {
            logger.error("An error occured:", e);
        }
    }
}
