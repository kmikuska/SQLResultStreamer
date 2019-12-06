package com.redrobin.sqlresultstreamer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class ExecuteSQL {

	public static void main(String[] args) {
	
		String pattern = "yyyy-MM-dd HH:mm:ss";
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);

		System.out.println("Started at: " + simpleDateFormat.format(new Date()));
		
		String query = null;
		
		try (InputStream input = ExecuteSQL.class.getClassLoader().getResourceAsStream("config.properties")){
			
			Properties props = new Properties();
			if (input == null) {
                System.out.println("Sorry, unable to find config.properties");
                return;
            }
			
			//load a properties file from class path, inside static method
            props.load(input);
			
			System.out.println("Loading SQL File: " + props.getProperty("sql.filepath"));
			query = loadSQLFile(props.getProperty("sql.filepath"));

			Connection conn = DriverManager.getConnection(props.getProperty("database.url"), props);
			conn.setAutoCommit(false);			
			
			Statement select = conn.createStatement();
			select.setFetchSize(50000);
			System.out.println("Executing Query");
			ResultSet rs = select.executeQuery(query);
			System.out.println("Got ResultSet");
			
			OutputStream out = new FileOutputStream(props.getProperty("output.path") + props.getProperty("output.filename") + ".csv");
			OutputStream out_special = new FileOutputStream(props.getProperty("output.path") + props.getProperty("output.filename") + "_SpecialChars.csv");
			
			StreamingCsvResultSetExtractor streamer = new StreamingCsvResultSetExtractor(out, out_special, StreamingCsvResultSetExtractor.RecordType.BOTH);

			System.out.println("Start Streaming Data");
			streamer.extractData(rs);
			
		} catch(IOException ioe){
			ioe.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("Finished at: " + simpleDateFormat.format(new Date()));
	}
	
	private static String loadSQLFile(String file) throws IOException{
		InputStream is = new FileInputStream(file);
		BufferedReader buf = new BufferedReader(new InputStreamReader(is));
		String line = buf.readLine(); StringBuilder sb = new StringBuilder();
		while(line != null){ 
			sb.append(line).append("\n");
			line = buf.readLine(); 
		}
		String fileAsString = sb.toString();
		System.out.println("Contents : " + fileAsString);
		
		return fileAsString;

	}

}
