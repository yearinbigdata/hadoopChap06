package com.example.log;

import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

public class CommonsLogTest {

	@Test
	public void test() {
		Log log = LogFactory.getLog(CommonsLogTest.class);
		
		System.out.println("Hello Log4J!");
		log.info("Hello Log4J!");
	}

}
