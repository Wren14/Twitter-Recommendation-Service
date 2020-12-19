package edu.cmu.cc.group.q1;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.*;

import java.nio.charset.Charset;

import org.apache.commons.io.FileUtils;

import edu.cmu.cc.group.q1.DriverQ1;

public class DriverQ1Test extends TestCase {
	
	private static final long N = 1284110893049L;
	
	private final static String TEAM_AWS_ACCOUNT_ID = "586374175774";
	
	private static final String
		EMPTY_CHAIN = "empty_chain.json",
		EMPTY_FILE = "empty_file.json",
		MALFORMED_INPUT = "malformed_input.json",
		MISSING_CHAIN = "missing_chain.json",
		MISSING_NEW_TX = "missing_new_tx.json",
		VALID_INPUT = "valid_input.json",
		
		BASE64_VALID_INPUT1 = "base64_valid_input1.txt",
		BASE64_VALID_INPUT2 = "base64_valid_input2.txt",
		BASE64_VALID_INPUT3 = "base64_valid_input3.txt",
		BASE64_VALID_INPUT4 = "base64_valid_input4.txt",
		BASE64_VALID_INPUT5 = "base64_valid_input5.txt",
		BASE64_VALID_INPUT6 = "base64_valid_input6.txt",
		
		BASE64_INVALID_INPUT1 = "base64_invalid_input1.txt",
		BASE64_INVALID_INPUT2 = "base64_invalid_input2.txt",
		BASE64_INVALID_INPUT3 = "base64_invalid_input3.txt",
		BASE64_INVALID_INPUT4 = "base64_invalid_input4.txt",
		BASE64_INVALID_INPUT5 = "base64_invalid_input5.txt",
		BASE64_INVALID_INPUT6 = "base64_invalid_input6.txt";
	
	/**
	 * Create the test case
	 *
	 * @param testName name of the test case
	 */
	public DriverQ1Test( String testName )
	{
		super( testName );
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite()
	{
		return new TestSuite( DriverQ1Test.class );
	}

	//=========================================================================
	// VALID INPUT
	public void testValidInputHandling1()
	{
		try {
			File file = new File(getClass().getClassLoader().getResource(BASE64_INVALID_INPUT1).getFile() );
			String input = FileUtils.readFileToString(file,  Charset.forName("UTF-8"));
			DriverQ1 client = new DriverQ1();
			String myOutput = client.handleInputRequest(input);
			
			
			assertNotNull(myOutput);
		} catch (IOException e) {
			e.printStackTrace();
			assertTrue( false );
		}
	}
	
	public void testValidInputHandling2()
	{
		try {
			File file = new File(getClass().getClassLoader().getResource(BASE64_VALID_INPUT2).getFile() );
			String input = FileUtils.readFileToString(file,  Charset.forName("UTF-8"));
			DriverQ1 client = new DriverQ1();
			String myOutput = client.handleInputRequest(input);
			
			assertNotNull(myOutput);
		} catch (IOException e) {
			e.printStackTrace();
			assertTrue( false );
		}
	}
	
	public void testValidInputHandling3()
	{
		try {
			File file = new File(getClass().getClassLoader().getResource(BASE64_VALID_INPUT3).getFile() );
			String input = FileUtils.readFileToString(file,  Charset.forName("UTF-8"));
			DriverQ1 client = new DriverQ1();
			String myOutput = client.handleInputRequest(input);
			
			String signature = "1033339299963";
			assertTrue(myOutput.contains(signature));
		} catch (IOException e) {
			e.printStackTrace();
			assertTrue( false );
		}
	}
	
	public void testValidInputHandling4()
	{
		try {
			File file = new File(getClass().getClassLoader().getResource(BASE64_VALID_INPUT4).getFile() );
			String input = FileUtils.readFileToString(file,  Charset.forName("UTF-8"));
			DriverQ1 client = new DriverQ1();
			String myOutput = client.handleInputRequest(input);
			
			String signature = "507638433091";
			assertTrue(myOutput.contains(signature));
			assertNotNull(myOutput);
		} catch (IOException e) {
			e.printStackTrace();
			assertTrue( false );
		}
	}
	
	public void testValidInputHandling5()
	{
		try {
			File file = new File(getClass().getClassLoader().getResource(BASE64_VALID_INPUT5).getFile() );
			String input = FileUtils.readFileToString(file,  Charset.forName("UTF-8"));
			DriverQ1 client = new DriverQ1();
			String myOutput = client.handleInputRequest(input);
			
			String signature = "443297284683";
			assertTrue(myOutput.contains(signature));
			assertNotNull(myOutput);
		} catch (IOException e) {
			e.printStackTrace();
			assertTrue( false );
		}
	}
	
	public void testValidInputHandling6()
	{
		try {
			File file = new File(getClass().getClassLoader().getResource(BASE64_VALID_INPUT6).getFile() );
			String input = FileUtils.readFileToString(file,  Charset.forName("UTF-8"));
			DriverQ1 client = new DriverQ1();
			String myOutput = client.handleInputRequest(input);
			
			//String signature = "443297284683";
			//assertTrue(myOutput.contains(signature));
			//assertNotNull(myOutput);
		} catch (IOException e) {
			e.printStackTrace();
			assertTrue( false );
		}
	}
	
	//=========================================================================
	// INVALID INPUT
	public void testInvalidInputHandling1() {
		try {
			File file = new File(getClass().getClassLoader().getResource(BASE64_INVALID_INPUT1).getFile() );
			String input = FileUtils.readFileToString(file,  Charset.forName("UTF-8"));
			DriverQ1 client = new DriverQ1();
			String myOutput = client.handleInputRequest(input);		
			
			assertTrue(myOutput.contains("INVALID"));
		} catch (IOException e) {
			e.printStackTrace();
			assertTrue( false );
		}
	}
	
	public void testInvalidInputHandling2()
	{
		try {
			File file = new File(getClass().getClassLoader().getResource(BASE64_INVALID_INPUT2).getFile() );
			String input = FileUtils.readFileToString(file,  Charset.forName("UTF-8"));
			DriverQ1 client = new DriverQ1();
			String myOutput = client.handleInputRequest(input);
			
			assertTrue(myOutput.contains("INVALID"));
		} catch (IOException e) {
			e.printStackTrace();
			assertTrue( false );
		}
	}
	
	public void testInvalidInputHandling3()
	{
		try {
			File file = new File(getClass().getClassLoader().getResource(BASE64_INVALID_INPUT3).getFile() );
			String input = FileUtils.readFileToString(file,  Charset.forName("UTF-8"));
			DriverQ1 client = new DriverQ1();
			String myOutput = client.handleInputRequest(input);
			
			assertTrue(myOutput.contains("INVALID"));
		} catch (IOException e) {
			e.printStackTrace();
			assertTrue( false );
		}
	}
	
	public void testInvalidInputHandling4()
	{
		try {
			File file = new File(getClass().getClassLoader().getResource(BASE64_INVALID_INPUT4).getFile() );
			String input = FileUtils.readFileToString(file,  Charset.forName("UTF-8"));
			DriverQ1 client = new DriverQ1();
			String myOutput = client.handleInputRequest(input);
			
			assertTrue(myOutput.contains("INVALID"));
		} catch (IOException e) {
			e.printStackTrace();
			assertTrue( false );
		}
	}
	
	public void testInvalidInputHandling5()
	{
		try {
			File file = new File(getClass().getClassLoader().getResource(BASE64_INVALID_INPUT5).getFile() );
			String input = FileUtils.readFileToString(file,  Charset.forName("UTF-8"));
			DriverQ1 client = new DriverQ1();
			String myOutput = client.handleInputRequest(input);
			
			assertTrue(myOutput.contains("INVALID"));
		} catch (IOException e) {
			e.printStackTrace();
			assertTrue( false );
		}
	}
	
	public void testInvalidInputHandling6()
	{
		try {
			File file = new File(getClass().getClassLoader().getResource(BASE64_INVALID_INPUT6).getFile() );
			String input = FileUtils.readFileToString(file,  Charset.forName("UTF-8"));
			DriverQ1 client = new DriverQ1();
			String myOutput = client.handleInputRequest(input);
			
			assertTrue(myOutput.contains("INVALID"));
		} catch (IOException e) {
			e.printStackTrace();
			assertTrue( false );
		}
	}
	
}
