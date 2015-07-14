package de.fu_berlin.agdb.importer_database.core;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import de.fu_berlin.agdb.nio_tools.AConnectionHandler;
import de.fu_berlin.agdb.nio_tools.NioClient;

public class DataCollectionTool extends AConnectionHandler{

	private static final Logger logger = LogManager.getLogger(DataCollectionTool.class);
	private Connection connection;
	
	public DataCollectionTool(String host, int port, Connection connection) throws IOException {
		this.connection = connection;
		
		NioClient nioClient = new NioClient(host, port, this);
		Thread thread = new Thread(nioClient);
		thread.start();
	}

	@Override
	public void handleReceivedData(byte[] data) {
		try {
			String jsonString = new String(data, "UTF-8");
			
			String statement = ""
				+ "INSERT INTO weather_data "
				+ "(data) "
				+ "VALUES "
				+ "(?) "
				+ "; ";
			
			PreparedStatement preparedStatement = connection.prepareStatement(statement);
			preparedStatement.setString(1, jsonString);
			preparedStatement.execute();
			preparedStatement.close();
		} catch (UnsupportedEncodingException e) {
			logger.error("Malformed data received", e);
		} catch (SQLException e) {
			logger.error("Problem while inserting data", e);
		}
	}
}
