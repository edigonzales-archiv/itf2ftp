package org.catais.interlis;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;


/**
 * Hello world!
 *
 */
public class App 
{
	private static Logger logger = Logger.getLogger(App.class);
	
    public static void main( String[] args )
    {    	
    	try {
			File tempDir = IOUtils.createTempDirectory("itf2ftp");
			InputStream is =  App.class.getResourceAsStream("log4j.properties");
			File log4j = new File(tempDir, "log4j.properties");
			IOUtils.copy(is, log4j);
	
			PropertyConfigurator.configure(log4j.getAbsolutePath());
			
			String propFileName = null;
			if (args.length > 0) {
				propFileName = (String) args[0];
				logger.debug("properties filename: : " + propFileName);
			} else {
				// works only here...
				String userDir = System.getProperty("user.dir");
				propFileName = userDir + "/Apps/itf2ftp.properties";
			}
	    	
	    	Itf2ftp itf2ftp = new Itf2ftp();
	    	
	    	try {
	    		itf2ftp.readProperties(propFileName);
	        	itf2ftp.upload();
	    	} catch (SQLException e) {
	    		e.printStackTrace();
	    	} catch (ClassNotFoundException e) {
	    		e.printStackTrace();
	    	} catch (FileNotFoundException e) {
	    		e.printStackTrace();
	    	} catch (IOException e) {
	    		e.printStackTrace();
	    	} 
	    	
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
    }
}
