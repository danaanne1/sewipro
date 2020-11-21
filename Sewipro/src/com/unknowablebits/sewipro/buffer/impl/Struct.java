package com.unknowablebits.sewipro.buffer.impl;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 * Basic implementation of a struct for a lazy loading, convert on write, in memory serialization mechanism.
 * 
 * @author Dana
 */
public class Struct {
	
	
	ByteBuffer backingBuffer;
	HashMap<String,Object> materializedRecords;
	HashMap<String,ByteBuffer> recordBuffers;
	HashMap<String,Object> newrecords;

	public Struct(ByteBuffer buffer) {
		this.backingBuffer = buffer;
	}
	public Struct() {
		this(ByteBuffer.allocate(5).put(Types.STRUCT).putInt(0));
	}
	
	public Object put(String key, Object value) {
		if (newrecords == null) {
			newrecords = new HashMap<>();
		}
		Object result = get(key);
		newrecords.put(key, value);
		return result;
	}
	
	public Set<String> keySet() {
		materialize();
		HashSet<String> result = new HashSet<String>(recordBuffers.keySet());
		if (newrecords!=null) result.addAll(newrecords.keySet());
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
		ByteBuffer buffer = recordBuffers.get(key);
		if (buffer == null)
			return null;
		Object obj = Types.fromByteBuffer(buffer);
		materializedRecords.put(key,obj);
		return obj;
	}
	

	public ByteBuffer toByteBuffer() {
		if (newrecords==null)
			return backingBuffer.rewind();
		LinkedHashMap<String, ByteBuffer> recordsOut = new LinkedHashMap<>(recordBuffers);
		for (String key: newrecords.keySet()) {
			Object obj = newrecords.get(key);
			if (obj==null)
				recordsOut.remove(key);
			else
				recordsOut.put(key, Types.toByteBuffer(obj));
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
		result.put(Types.STRUCT);
		
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
