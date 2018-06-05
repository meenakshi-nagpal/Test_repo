package jobs.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;





import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import play.Play;
import models.storageapp.AppConfigProperty;

import java.util.logging.Logger;


public final class DatabaseConnection {

	private DatabaseConnection() {}
	
	private static Logger log = Logger.getLogger(DatabaseConnection.class.getName());
	
	public static Connection getUDASDBConnection() throws Exception{
		Connection connectionObj = null;
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			
			String dbUrl = Play.application().configuration().getString("db.default.url");
			String dbUser = Play.application().configuration().getString("db.default.user");
			String dbPassword = Play.application().configuration().getString("db.default.password");
			
			connectionObj = DriverManager.getConnection(dbUrl, dbUser, dbPassword);		
		} catch (ClassNotFoundException cnfe) {			
			cnfe.printStackTrace();
			throw new Exception("Data base connection to UDAS Database not be established.");
		}catch (SQLException sqle) {			
			sqle.printStackTrace();
			throw new Exception("Data base connection to UDAS Database can not be established.");
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("Data base connection to UDAS Database can not be established.");
		}	
		return connectionObj;
	}
	
	/**
	 * Closes the provided ResultSets	 
	 * @param resultSets
	 * ResultSets that should be closed
	 */
	public static void close(ResultSet... resultSets) {

		if (resultSets == null)
			return;

		for (ResultSet resultSet : resultSets)
			if (resultSet != null)
				try {
					resultSet.close();
				} catch (SQLException e) {					
					e.printStackTrace();
				}
	}

	/**
	 * Closes the provided Statements	  
	 * @param statements
	 * Statements that should be closed
	 */
	public static void close(Statement... statements) {
		/*
		 * No need to create methods for PreparedStatement and
		 * CallableStatement, because they extend Statement.
		 */

		if (statements == null)
			return;

		for (Statement statement : statements)
			if (statement != null)
				try {
					statement.close();
				} catch (SQLException e) {					
					e.printStackTrace();
				}
	}

	/**
	 * Closes the provided Connections	 
	 * @param connections
	 * Connections that should be closed
	 */
	public static void close(Connection... connections) {

		if (connections == null)
			return;

		for (Connection connection : connections)
			if (connection != null)
				try {
					connection.close();
				} catch (SQLException e) {				
					e.printStackTrace();
				}
	}
}
