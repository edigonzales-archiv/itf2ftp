package org.catais.interlis;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Properties;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.log4j.Logger;



public class Itf2ftp {
	
	private static Logger logger = Logger.getLogger(Itf2ftp.class);
	
	private String sourceDir = "/tmp/itf/";
//	private String sourceDir = "/opt/wwwroot/sogis/daten/kva/av/itf/";

	private String destinationDir = "Geodaten/av/so_tmp/";
	private String deliveryDate = null;
	
	private boolean uploadAllFiles = false;
	
	private String dbhost = "localhost";
	private String dbname = "xanadu";
	private String dbport = "5432";
	private String dbuser = "mspublic";
	private String dbpwd = "";
	
	private String ftphost = "localhost";
	private int ftpport = 21;
	private String ftpuser = "foo";
	private String ftppwd = "bar";
	
	
	public Itf2ftp() {
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -1);
		
		SimpleDateFormat df = new SimpleDateFormat( "yyyy-MM-dd" );
		deliveryDate = df.format(cal.getTime());
		logger.debug("Delivery date (constructor): " + deliveryDate);
		logger.debug("Source directory (constructor): " + sourceDir);
	}
	
	
	public void readProperties(String fileName) throws FileNotFoundException, IOException {
		Properties properties = new Properties();
		BufferedInputStream stream = new BufferedInputStream(new FileInputStream(fileName));
		properties.load(stream);
		stream.close();

		String dhost = properties.getProperty("dbhost");
		String dname = properties.getProperty("dbname");
		String dport = properties.getProperty("dbport");
		String duser = properties.getProperty("dbuser");
		String dpwd = properties.getProperty("dbpwd");
		
		if (dhost != null) {
			dbhost = dhost;
			logger.debug(dbhost);
		}
		
		if (dname != null) {
			dbname = dname;
			logger.debug(dbname);
		}
		
		if (dport != null) {
			dbport = dport;
			logger.debug(dbport);
		}
		
		if (duser != null) {
			dbuser = duser;
			logger.debug(dbuser);
		}
		
		if (dpwd != null) {
			dbpwd = dpwd;
			logger.debug(dbpwd);
		}
		
		
		String fhost = properties.getProperty("ftphost");
		String fport = properties.getProperty("ftpport");
		String fuser = properties.getProperty("ftpuser");
		String fpwd = properties.getProperty("ftppwd");
		
		
		if (fhost != null) {
			ftphost = fhost;
			logger.debug(ftphost);
		}
		
		if (fport != null) {
			ftpport = Integer.parseInt(fport);
			logger.debug(ftpport);
		}
		
		if (fuser != null) {
			ftpuser = fuser;
			logger.debug(ftpuser);
		}
		
		if (fpwd != null) {
			ftppwd = fpwd;
			logger.debug(ftppwd);
		}
		
		
		String dstDir = properties.getProperty("destinationDir");
		String srcDir = properties.getProperty("sourceDir");
		
		if (dstDir != null) {
			destinationDir = dstDir;
			logger.debug(destinationDir);
		}
		
		if (srcDir != null) {
			sourceDir = srcDir;
			logger.debug(sourceDir);
		}
		
		
		String delDate = properties.getProperty("deliveryDate");
		String upAll = properties.getProperty("uploadAll");
		
		if (delDate != null) {
			deliveryDate = delDate;
			logger.debug(deliveryDate);
		}
		
		if (upAll != null) {
			uploadAllFiles = Boolean.parseBoolean(upAll);
			logger.debug(uploadAllFiles);
		}
	}
		
	
	private ArrayList readLocalFiles() {
		File dir = new File(sourceDir);
        String[] files = dir.list(new FilenameFilter() {
                public boolean accept(File d, String name) {
                        return name.toLowerCase().endsWith(".itf"); 
                }
        });
        logger.debug("ITF filelist (length): " + files.length);
        
        ArrayList<String> fileList = new ArrayList();
        for (int i = 0; i < files.length; i++) {
        	fileList.add(files[i].substring(0, files[i].indexOf(".")));
        }
        
        return fileList;
	}
	
	
	private ArrayList readDeliveredFiles() throws ClassNotFoundException, SQLException, IOException {
		
		ArrayList<String> deliveryList = new ArrayList();
		
//		String dbhost = "srsofaioi4531";
//		String dbname = "sogis";
//		String dbport = "5432";
//		String dbuser = "mspublic";
//		String dbpwd = "";
		
		Class.forName("org.postgresql.Driver");
		logger.debug("PostgreSQL JDBC Driver Registered.");
		
		Connection conn = DriverManager.getConnection("jdbc:postgresql://"+dbhost+"/"+dbname, dbuser, dbpwd);
		
		if (conn != null) {
			Statement s = null;
			s = conn.createStatement();
			
			ResultSet rs = null;
			rs = s.executeQuery("SELECT * FROM avdpool.lieferungen WHERE date(datum) = '" + deliveryDate + "'");
			while (rs.next()) {
				String filename = rs.getString("filename");
				deliveryList.add(filename.substring(0, filename.indexOf(".")));				
			}
			logger.debug(deliveryList);
			s.close();
			conn.close();
		}
		
		return deliveryList;
	}
	
	
	public void upload() throws ClassNotFoundException, SQLException, IOException {
		
		ArrayList fileList = new ArrayList();
		
		if (uploadAllFiles == false) {
			fileList = readDeliveredFiles();
		} else {
			fileList = readLocalFiles();
		}
		
		FTPClient ftpClient = new FTPClient();
		FileInputStream fis = null;
		boolean resultOk = true;
		
		ftpClient.setDataTimeout(5);

		ftpClient.connect(ftphost, ftpport);
		logger.debug(ftpClient.getReplyString());
		
		resultOk &= ftpClient.login(ftpuser, ftppwd);
		logger.debug(ftpClient.getReplyString());
		
		for (int i = 0; i < fileList.size(); i++) {
			String sourceFileName = sourceDir + fileList.get(i) + ".itf";
			logger.debug(sourceFileName);
			
			String destFileName = destinationDir + fileList.get(i) + ".itf";
//			String destFileName = deliveryList.get(i) + ".itf";
			logger.debug(destFileName);
			
			fis = new FileInputStream(sourceFileName);
			
			ftpClient.setFileType(ftpClient.ASCII_FILE_TYPE);
			ftpClient.enterLocalPassiveMode();
			
			resultOk &= ftpClient.storeFile( destFileName, fis );
			logger.debug(ftpClient.getReplyString());
		}
		
		resultOk &= ftpClient.logout();
		logger.debug(ftpClient.getReplyString());
		
		ftpClient.disconnect();
		logger.debug("ftpClient disconnected");

	}
	
	
}
