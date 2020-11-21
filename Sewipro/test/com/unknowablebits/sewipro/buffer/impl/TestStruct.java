package com.unknowablebits.sewipro.buffer.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.ByteBuffer;

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
		Struct r = testStruct();
		
		ByteBuffer b = r.toByteBuffer();
		
		Struct r2 = new Struct(b);

		assertEquals((byte)1,r2.get("Byte"));
		assertEquals('c',r2.get("Char"));
		assertEquals(2d,r2.get("Double"));
		assertEquals(3f,r2.get("Float"));
		assertEquals(4,r2.get("Int"));
		assertEquals(5l,r2.get("Long"));
		assertEquals("Hello World",r2.get("String"));
		assertNull(r2.get("Empty"));
	}

	private Struct testStruct() {
		Struct r = new Struct();
		
		r.put("Byte", (byte)1);
		r.put("Char", 'c');
		r.put("Double", 2d);
		r.put("Float", 3f);
		r.put("Int", 4);
		r.put("Long", 5l);
		r.put("Short", (short)6);
		r.put("String", "Hello World");
		r.put("Empty",null);
		return r;
	}

	@Test
	void structWithinStruct() {
		Struct r = testStruct();
		r.put("Struct",testStruct());

		Struct r2 = new Struct(r.toByteBuffer());
		assertEquals((byte)1,r2.get("Byte"));
		assertEquals('c',r2.get("Char"));
		assertEquals(2d,r2.get("Double"));
		assertEquals(3f,r2.get("Float"));
		assertEquals(4,r2.get("Int"));
		assertEquals(5l,r2.get("Long"));
		assertEquals("Hello World",r2.get("String"));
		assertNull(r2.get("Empty"));
		
		Struct r3 = (Struct)r2.get("Struct");
		assertEquals((byte)1,r3.get("Byte"));
		assertEquals('c',r3.get("Char"));
		assertEquals(2d,r3.get("Double"));
		assertEquals(3f,r3.get("Float"));
		assertEquals(4,r3.get("Int"));
		assertEquals(5l,r3.get("Long"));
		assertEquals("Hello World",r3.get("String"));
		assertNull(r3.get("Empty"));
		assertNull(r3.get("Struct"));
	}
}
