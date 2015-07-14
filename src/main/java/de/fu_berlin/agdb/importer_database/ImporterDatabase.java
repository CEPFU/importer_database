package de.fu_berlin.agdb.importer_database;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import de.fu_berlin.agdb.importer.tools.ConnectionManager;
import de.fu_berlin.agdb.importer_database.core.DataCollectionTool;
import de.fu_berlin.agdb.importer_database.core.DatabaseSetupTool;

public class ImporterDatabase {
	
	private static final Logger logger = LogManager.getLogger(ImporterDatabase.class);
	
	private static final String DATABASE_HOST = "10.10.10.105";
	private static final String DATABASE_PORT = "5432";
	private static final String DATABASE = "ems";
	private static final String USER = "ems";
	private static final String PASSWORD = "ems";
	
	public static void main(String[] args){
		BasicConfigurator.configure();
		ConnectionManager connectionManager = new ConnectionManager(DATABASE_HOST + ":" + DATABASE_PORT, DATABASE, USER, PASSWORD, 10);
		
		try {
			DatabaseSetupTool.createMetaDataTable(connectionManager);
			DatabaseSetupTool.createDWDMetaDataTable(connectionManager);
			DatabaseSetupTool.createWeatherDataTable(connectionManager);
			
			DatabaseSetupTool.insertDWDMetaInformation(connectionManager);
			
			new DataCollectionTool("10.10.10.50", 9977, connectionManager);
		} catch (Exception e) {
			logger.error("An error occured:", e);
		}
	}
}
