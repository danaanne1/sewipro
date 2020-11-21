package com.unknowablebits.sewipro.buffer.impl;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;

import javax.imageio.stream.IIOByteBuffer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestStruct {

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
	}

	@BeforeEach
	void setUp() throws Exception {
	}

	@AfterEach
	void tearDown() throws Exception {
	}

	@Test
	void basicSerialization() {

		Struct r = new Struct();
		
		r.put("Hello World", "Hello World Data");

		ByteBuffer b = r.toByteBuffer();
		
		Struct r2 = new Struct(b);
		
		System.out.println((String)r2.get("Hello World"));
	
	}

}
