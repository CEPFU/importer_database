package de.fu_berlin.agdb.importer_database;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.BasicConfigurator;

import de.fu_berlin.agdb.importer.tools.ConnectionManager;
import de.fu_berlin.agdb.importer_database.core.DataCollectionTool;
import de.fu_berlin.agdb.importer_database.core.DatabaseSetupTool;

public class ImporterDatabase {
	
	private static final String DATABASE_HOST = "10.10.10.105";
	private static final String DATABASE_PORT = "5432";
	private static final String DATABASE = "ems";
	private static final String USER = "ems";
	private static final String PASSWORD = "ems";
	
	public static void main(String[] args) throws SQLException, IOException {
		BasicConfigurator.configure();
		ConnectionManager connectionManager = new ConnectionManager(DATABASE_HOST + ":" + DATABASE_PORT, DATABASE, USER, PASSWORD);
		
		DatabaseSetupTool.createMetaDataTable(connectionManager.getConnection(connectionManager));
		DatabaseSetupTool.createDWDMetaDataTable(connectionManager.getConnection(connectionManager));
		DatabaseSetupTool.createWeatherDataTable(connectionManager.getConnection(connectionManager));
		
//		DatabaseSetupTool.insertDWDMetaInformation(connectionManager.getConnection(connectionManager));
		
		new DataCollectionTool("10.10.10.50", 9977, connectionManager.getConnection(connectionManager));
		
		connectionManager.closeConnection(connectionManager);
	}
}
