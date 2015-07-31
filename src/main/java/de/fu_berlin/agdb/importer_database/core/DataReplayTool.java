package de.fu_berlin.agdb.importer_database.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import de.fu_berlin.agdb.importer.tools.ConnectionManager;
import de.fu_berlin.agdb.importer_database.ImporterDatabase;
import de.fu_berlin.agdb.nio_tools.AConnectionHandler;
import de.fu_berlin.agdb.nio_tools.NioServer;

public class DataReplayTool {

	private static final Logger logger = LogManager.getLogger(DataReplayTool.class);
	
	public DataReplayTool(int replayDataPort, ConnectionManager connectionManager, Long replayIntervall) throws Exception {
		NioServer nioServer = new NioServer(replayDataPort, new AConnectionHandler() {
			@Override
			public void handleReceivedData(byte[] data) {
				throw new UnsupportedOperationException("This server shouldn't receive any data, "
						+ "something suspicious is going on.");
			}
		});
		Thread thread = new Thread(nioServer);
		thread.start();
		
		while(true){
			publishEvents(connectionManager, nioServer);
			logger.debug("Replayed data! Waiting " + replayIntervall + "ms to replay again.");
			
			long jobDoneTime = System.currentTimeMillis();
			long timeInLoop;
			while(jobDoneTime + replayIntervall > (timeInLoop = System.currentTimeMillis())){
				Thread.sleep(jobDoneTime + replayIntervall - timeInLoop);
			}
		}
	}

	private void publishEvents(ConnectionManager connectionManager, NioServer nioServer) throws Exception {
		ArrayList<Long> weatherDataIds= getWeatherDataIds(connectionManager);
		
		for (Long weatherDataId : weatherDataIds) {
			byte[] weatherData = getWeatherData(weatherDataId, connectionManager);
			nioServer.bordcastData(weatherData);
		}
	}

	private byte[] getWeatherData(Long weatherDataId, ConnectionManager connectionManager) throws Exception {
		byte[] weatherData = null;
		
		Connection connection = connectionManager.requestConnection();
		
		PreparedStatement preparedStatement = connection.prepareStatement(""
				+ "SELECT data "
				+ "FROM weather_data "
				+ "WHERE weather_data_id = ? "
				+ ";");
		preparedStatement.setLong(1, weatherDataId);
		ResultSet resultSet = preparedStatement.executeQuery();
		if(resultSet.next()){
			String data = resultSet.getString("data");
			weatherData = data.getBytes("UTF-8");
		}
		
		connectionManager.returnConnectionToPool(connection);
		return weatherData;
	}

	private ArrayList<Long> getWeatherDataIds(ConnectionManager connectionManager) throws Exception {
		ArrayList<Long> weatherDataIds = new ArrayList<Long>();
		
		Connection connection = connectionManager.requestConnection();
		
		PreparedStatement preparedStatement = connection.prepareStatement(""
				+ "SELECT weather_data_id "
				+ "FROM weather_data "
				+ ";");
		ResultSet resultSet = preparedStatement.executeQuery();
		while(resultSet.next()){
			weatherDataIds.add(resultSet.getLong("weather_data_id"));
		}
		
		connectionManager.returnConnectionToPool(connection);
		return weatherDataIds;
	}
}
