package com.unknowablebits.sewipro.buffer.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.function.Function;

import javax.imageio.stream.IIOByteBuffer;

/**
 * Basic implementation of a struct for a lazy loading, convert on write, in memory serialization mechanism.
 * 
 * @author Dana
 */
public class Struct {
	
	private static final byte STRUCT = 0x01;
	private static final byte ARRAY = 0x02;
	private static final byte BYTE = 0x03;
	private static final byte CHAR = 0x04;
	private static final byte DOUBLE = 0x05;
	private static final byte FLOAT = 0x06;
	private static final byte INT = 0x07;
	private static final byte LONG = 0x08;
	private static final byte SHORT = 0x09;
	private static final byte STRING = 0x10;
	private static final byte OBJECT = 0x11;
	
	
	private static final Map<Class<?>,Function<Object, ByteBuffer>> converters = new HashMap<>();
	
	static {
		converters.put(Struct.class, v->((Struct)v).toByteBuffer());
		converters.put(Byte.class, v->ByteBuffer.allocate(2).put(BYTE).put((Byte)v).rewind());
		converters.put(Character.class, v->ByteBuffer.allocate(3).put(CHAR).putChar((Character)v).rewind());
		converters.put(Double.class, v->ByteBuffer.allocate(9).put(DOUBLE).putDouble((Double)v).rewind());
		converters.put(Float.class, v->ByteBuffer.allocate(5).put(FLOAT).putFloat((Float)v).rewind());
		converters.put(Integer.class, v->ByteBuffer.allocate(5).put(INT).putInt((Integer)v).rewind());
		converters.put(Long.class, v->ByteBuffer.allocate(9).put(LONG).putLong((Long)v).rewind());
		converters.put(Short.class, v->ByteBuffer.allocate(3).put(SHORT).putShort((Short)v).rewind());
		converters.put(Byte.TYPE, converters.get(Byte.class));
		converters.put(Character.TYPE, converters.get(Character.class));
		converters.put(Double.TYPE, converters.get(Double.class));
		converters.put(Float.TYPE, converters.get(Float.class));
		converters.put(Integer.TYPE, converters.get(Integer.class));
		converters.put(Long.TYPE, converters.get(Long.class));
		converters.put(Short.TYPE, converters.get(Short.class));
		converters.put(String.class, v-> {
			ByteBuffer b = ByteBuffer.allocate(((String)v).length()*2+1).put(STRING);
			b.asCharBuffer().put((String)v);
			return b.rewind();
		});
	}
	
	ByteBuffer backingBuffer;
	HashMap<String,Object> materializedRecords;
	HashMap<String,ByteBuffer> recordBuffers;
	HashMap<String,Object> newrecords;

	public Struct(ByteBuffer buffer) {
		this.backingBuffer = buffer;
	}
	public Struct() {
		this(ByteBuffer.allocate(5).put(STRUCT).putInt(0));
	}
	
	public Object put(String key, Object value) {
		if (newrecords == null) {
			newrecords = new HashMap<>();
		}
		Object result = get(key);
		newrecords.put(key, value);
		return result;
	}
	
	private void materialize() {
		if (recordBuffers!=null) return;

		recordBuffers = new HashMap<>();
		materializedRecords = new HashMap<>();

		// read all of the keys
		String [] keys = new String[backingBuffer.position(1).getInt()];
		for (int i = 0; i < keys.length; i++) {
			int len = backingBuffer.getInt();
			keys[i] = backingBuffer.slice().limit(len).asCharBuffer().toString();
			backingBuffer.position(backingBuffer.position()+len);
		}

		// read all of the sizes
		int [] sizes = new int[keys.length];
		for (int i = 0; i < keys.length; i++) {
			sizes[i] = backingBuffer.getInt();
		}

		// build the map
		for (int i = 0; i < keys.length; i++) {
			recordBuffers.put(keys[i], backingBuffer.slice().limit(sizes[i]));
			backingBuffer.position(backingBuffer.position()+sizes[i]);
		}
		
	}

	public Object get(String key) {
		if (newrecords!=null && newrecords.containsKey(key)) {
			return newrecords.get(key);
		}
		materialize();
		if (materializedRecords.containsKey(key)) {
			return materializedRecords.get(key);
		}
		Object obj;
		ByteBuffer buffer = recordBuffers.get(key);
		if (buffer == null)
			return null;
		switch (buffer.rewind().get()) {
		case STRUCT:
			obj=new Struct(buffer);
			break;
//			case ARRAY:
//				records.put(key,obj=new Array(buffer.slice().limit(buffer.getInt())));
//				break;
		case BYTE:
			obj=buffer.get();
			break;
		case CHAR:
			obj=buffer.getChar();
			break;
		case DOUBLE:
			obj=buffer.getDouble();
			break;
		case FLOAT:
			obj=buffer.getFloat();
			break;
		case INT:
			obj=buffer.getInt();
			break;
		case LONG:
			obj=buffer.getLong();
			break;
		case SHORT:
			obj=buffer.getShort();
			break;
		case STRING:
			obj=buffer.asCharBuffer().toString();
			break;
		case OBJECT:
			obj=readSerializedObjectFromBuffer(buffer.slice());
			break;
		default: 
			throw new IllegalStateException();
		}
		materializedRecords.put(key,obj);
		return obj;
	}
	
	private static final Object readSerializedObjectFromBuffer(ByteBuffer buffer) {
		// TODO : make hasArray() friendly
		byte [] b = new byte[buffer.limit()];
		buffer.get(b);
		try ( ObjectInputStream oin = new ObjectInputStream(new ByteArrayInputStream(b)) ) {
			return oin.readObject();
		} catch (ClassNotFoundException | IOException e ) {
			throw new RuntimeException(e);
		}
	}

	private static final ByteBuffer writeSerializedObjectToBuffer(Object obj) {
		try (	ByteArrayOutputStream bout = new ByteArrayOutputStream();
				ObjectOutputStream oout = new ObjectOutputStream(bout) ) {
			oout.writeObject(obj);
			oout.close();
			return ByteBuffer.wrap(bout.toByteArray());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public ByteBuffer toByteBuffer() {
		if (newrecords==null)
			return backingBuffer.rewind();
		LinkedHashMap<String, ByteBuffer> recordsOut = new LinkedHashMap<>(recordBuffers);
		for (String key: newrecords.keySet()) {
			Object obj = newrecords.get(key);
			if (converters.containsKey(obj.getClass())) {
				recordsOut.put(key, converters.get(obj.getClass()).apply(obj));
			} else {
				recordsOut.put(key, writeSerializedObjectToBuffer(obj));
			}
		}

		// calculate size
		ByteBuffer result = ByteBuffer.allocate(
				1 // struct type
				+ 4 // key count
				+ recordsOut.keySet().stream().map(v->4+v.length()*2).reduce(0, (a,b)->a+b) // size of all the keys
				+ (recordsOut.size() * 4) // size of all the sizes
				+ recordsOut.values().stream().map(v->v.limit()).reduce(0,(a,b)->a+b) // size of all the actuall data buffers
		);

		// write the struct descriptor
		result.put(STRUCT);
		
		
		// write the keys
		result.putInt(recordsOut.size());
		recordsOut.keySet().forEach(key-> {
			result.putInt(key.length()*2);
			result.asCharBuffer().put(key);
			result.position(result.position()+key.length()*2);
		});

		// write the sizes
		recordsOut.values().forEach(buffer-> result.putInt(buffer.limit()));

		// write the buffers
		recordsOut.values().forEach(buffer-> result.put(buffer.rewind()));
		
		return result.rewind();
	}
	
}
