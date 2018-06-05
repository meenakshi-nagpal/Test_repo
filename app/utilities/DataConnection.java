package utilities;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.SQLException;


import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import java.util.logging.Logger;


public final class DataConnection {

	private DataConnection() {}
	
	private static Logger log = Logger.getLogger(DataConnection.class.getName());
	

	public static DataSource getAimsDataSource() throws Exception {
		return getDataSource("AIMSDB");
		
	}

	public static DataSource getDataSource(String dsName) throws Exception {

		DataSource ds = null;
		try {
			Context ctx = new InitialContext();
			ds = (DataSource) ctx.lookup(dsName);
		} catch (Exception e) {
			log.info("Error getting " + dsName + " DataSource-"+ e);
		}
		return ds;

	}

	public static void closeConnection(Connection connection) {
		try {
			if (connection != null)
				connection.close();
		} catch (SQLException e) {
		}
	}

	public static void closeStatement(Statement statement) {
		if (statement != null)
			try {
				statement.close();
			} catch (SQLException e) {
			}
	}

}
