package de.fu_berlin.agdb.importer_database.core;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.SocketException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.StringTokenizer;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

public class DWDMetaDataFileGatherer {
	
	public static final String server = "ftp-cdc.dwd.de";
	public static final String dataDirectory = "/pub/CDC/observations_germany/climate/daily/kl/recent/";

	public void gatherAndInjectMetaData(Connection connection) throws IOException, SQLException {
		File file = new File("station_meta_data.tmp");
		
		FTPClient ftpClient = getFTPClient();
		FTPFile[] listFiles = ftpClient.listFiles();
		for (FTPFile ftpFile : listFiles) {
			if(ftpFile.getName().equals("KL_Tageswerte_Beschreibung_Stationen.txt")){
				retriveFTPFile(ftpClient, ftpFile, file);
			}
		}
		shutDownFTPClient(ftpClient);
				
		if(file.exists()){
			BufferedReader reader;
			reader = new BufferedReader(new FileReader(file));
			//ignore first two lines because there is no information
			String line = reader.readLine(); 
			line = reader.readLine(); 
			//ignore first two lines because there is no information
			
			while((line = reader.readLine()) != null && line.length() > 1){
				handleLine(line, connection);
			}
			reader.close();
			file.delete();
		}
	}


	private void handleLine(String line, Connection connection) throws SQLException {
		connection.setAutoCommit(false);
		
		StringTokenizer tokenizer = new StringTokenizer(line);
		//stations_id
		Long stationId = Long.valueOf(tokenizer.nextToken());
		//von_datum
		String stringFromDate = tokenizer.nextToken();
		stringFromDate = stringFromDate.substring(0, 4) + "-" + stringFromDate.substring(4, 6)+ "-" + stringFromDate.substring(6);
		Date fromDate = Date.valueOf(stringFromDate);
		//bis_datum
		String stringUntilDate = tokenizer.nextToken();
		stringUntilDate = stringUntilDate.substring(0, 4) + "-" + stringUntilDate.substring(4, 6)+ "-" + stringUntilDate.substring(6);
		Date untilDate = Date.valueOf(stringUntilDate);
		//Stationshoehe
		int stationHeight = Integer.parseInt(tokenizer.nextToken());
		//geoBreite
		Double latitude = Double.valueOf(tokenizer.nextToken());
		//geoLaenge
		Double longitude = Double.valueOf(tokenizer.nextToken());
		//Stationsname
		String stationName = tokenizer.nextToken();
		//Bundesland
		String federalState = tokenizer.nextToken();
		
		String locationStatement = ""
				+ "INSERT INTO location_meta_data "
				+ "(location_position, location_description) "
				+ "  SELECT ST_MakePoint(?,?), ? "
				+ "  WHERE NOT EXISTS ( "
				+ "    SELECT station_id "
				+ "    FROM dwd_meta_data "
				+ "    WHERE station_id = ?) "
				+ "RETURNING location_id "
				+ "; ";
		PreparedStatement locationPreparedStatemend = connection.prepareStatement(locationStatement);
		locationPreparedStatemend.setDouble(1, longitude);
		locationPreparedStatemend.setDouble(2, latitude);
		locationPreparedStatemend.setString(3, stationName);
		locationPreparedStatemend.setLong(4, stationId);
		locationPreparedStatemend.execute();
		
		ResultSet resultSet = locationPreparedStatemend.getResultSet();
		resultSet.next();
		long locationId = resultSet.getLong("location_id");
		resultSet.close();
		locationPreparedStatemend.close();
		
		String dwdStatement = ""
				+ "INSERT INTO dwd_meta_data "
				+ "(location_id, station_id, from_date, until_date, station_height, federal_state) "
				+ "  SELECT ?, ?, ?, ?, ?, ? "
				+ "  WHERE NOT EXISTS ( "
				+ "    SELECT station_id "
				+ "    FROM dwd_meta_data "
				+ "    WHERE station_id = ?); ";
		PreparedStatement dwdPreparedStatemend = connection.prepareStatement(dwdStatement);
		dwdPreparedStatemend.setLong(1, locationId);
		dwdPreparedStatemend.setLong(2, stationId);
		dwdPreparedStatemend.setDate(3, fromDate);
		dwdPreparedStatemend.setDate(4, untilDate);
		dwdPreparedStatemend.setInt(5, stationHeight);
		dwdPreparedStatemend.setString(6, federalState);
		dwdPreparedStatemend.setLong(7, stationId);
		dwdPreparedStatemend.execute(); 
		dwdPreparedStatemend.close();
		
		connection.setAutoCommit(true);
	}
	
	private FTPClient getFTPClient() throws SocketException, IOException{
		FTPClient ftpClient = new FTPClient();
		ftpClient.connect(server);
		ftpClient.login("anonymous", "");
		ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
		ftpClient.changeWorkingDirectory(dataDirectory);
		return ftpClient;
	}
	
	private void shutDownFTPClient(FTPClient ftpClient) throws IOException {
		ftpClient.logout();
		ftpClient.disconnect();
	}
	
	private boolean retriveFTPFile(FTPClient ftpClient, FTPFile ftpFile, File file) throws FileNotFoundException, IOException {
		OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file));
		boolean success = ftpClient.retrieveFile(ftpFile.getName(), outputStream);
		outputStream.close();
		return success;
	}
}
