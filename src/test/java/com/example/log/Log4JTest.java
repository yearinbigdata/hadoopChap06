
package com.example.log;

import static org.junit.Assert.*;

import org.apache.log4j.Logger;
import org.junit.Test;

public class Log4JTest {

	@Test
	public void test() {
		Logger log = Logger.getLogger(Log4JTest.class);
		
		System.out.println("Hello Log4J!");
		log.info("Hello Log4J!");
	}

}

