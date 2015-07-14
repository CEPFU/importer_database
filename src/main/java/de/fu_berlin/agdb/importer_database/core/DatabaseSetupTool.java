package de.fu_berlin.agdb.importer_database.core;

import java.sql.Connection;
import java.sql.PreparedStatement;

import de.fu_berlin.agdb.importer.tools.ConnectionManager;

public final class DatabaseSetupTool {
	public static void createMetaDataTable(ConnectionManager connectionManager) throws Exception{
		Connection connection = connectionManager.requestConnection();
		String mainTableStatement = "CREATE TABLE public.location_meta_data" + 
				"( " +
					"location_id serial NOT NULL, " +
					"location_position geometry NOT NULL, " +
					"location_description character varying NOT NULL, " +
					"CONSTRAINT location_meta_data_primary_key PRIMARY KEY (location_id) " +
				") " +
				"WITH ( " +
					"OIDS = FALSE " +
				") " +
				"; " ;
		
		PreparedStatement mainTablePreparedStatemend = connection.prepareStatement(mainTableStatement);
		mainTablePreparedStatemend.execute();
		mainTablePreparedStatemend.close();
		connectionManager.returnConnectionToPool(connection);
	}
	
	public static void createDWDMetaDataTable(ConnectionManager connectionManager) throws Exception{
		Connection connection = connectionManager.requestConnection();
		String dwdTableStatement = "CREATE TABLE public.dwd_meta_data" + 
				"( " +
					"location_id bigint NOT NULL, " +
					"station_id bigint NOT NULL, " +
					"from_date date NOT NULL, " +
					"until_date date NOT NULL, " +
					"station_height integer NOT NULL, " +
					"federal_state character varying NOT NULL, " +
					"CONSTRAINT dwd_meta_data_primary_key PRIMARY KEY (location_id, station_id), " +
					"CONSTRAINT dwd_meta_data_key_1 FOREIGN KEY (location_id) REFERENCES public.location_meta_data (location_id) ON UPDATE CASCADE ON DELETE CASCADE " +
				") " +
				"WITH ( " +
					"OIDS = FALSE " +
				") " +
				"; " ;
		
		PreparedStatement dwdTablePreparedStatemend = connection.prepareStatement(dwdTableStatement);
		dwdTablePreparedStatemend.execute();
		dwdTablePreparedStatemend.close();
		connectionManager.returnConnectionToPool(connection);
	}
	
	public static void insertDWDMetaInformation(ConnectionManager connectionManager) throws Exception{
		Connection connection = connectionManager.requestConnection();
		DWDMetaDataFileGatherer dwdMetaDataFileGatherer = new DWDMetaDataFileGatherer();
		dwdMetaDataFileGatherer.gatherAndInjectMetaData(connection);
		connectionManager.returnConnectionToPool(connection);
	}
	
	public static void createWeatherDataTable(ConnectionManager connectionManager) throws Exception{
		Connection connection = connectionManager.requestConnection();
		String weatherDataTableStatement = "CREATE TABLE public.weather_data" + 
				"( " +
					"weather_data_id serial NOT NULL, " +
					"data json NOT NULL, " +
					"CONSTRAINT weather_data_primary_key PRIMARY KEY (weather_data_id) " +
				") " +
				"WITH ( " +
					"OIDS = FALSE " +
				") " +
				"; " ;
		
		PreparedStatement weatherDataTablePreparedStatemend = connection.prepareStatement(weatherDataTableStatement);
		weatherDataTablePreparedStatemend.execute();
		weatherDataTablePreparedStatemend.close();
		connectionManager.returnConnectionToPool(connection);
	}
}
