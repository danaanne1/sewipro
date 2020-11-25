package com.unknowablebits.sewipro.buffer.impl;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Basic implementation of a struct for a lazy loading, convert on write, in memory serialization mechanism.
 * 
 * @author Dana
 */
public class Struct {
	ByteBuffer backingBuffer;
	HashMap<String,Record> records;

	public Struct(ByteBuffer buffer) {
		this.backingBuffer = buffer;
	}

	public Struct() {
		this(ByteBuffer.allocate(5).put(Types.STRUCT).putInt(0));
	}
	
	public Object put(String key, Object value) {
		materialize();
		Object oldValue = get(key);
		records.put(key, new Record().withValue(value));
		return oldValue;
	}
	
	public Set<String> keySet() {
		materialize();
		return records.keySet();
	}
	
	private void materialize() {
		if (records!=null) return;
		
		records = new HashMap<>();
		
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
			records.put(keys[i], new Record(backingBuffer.slice().limit(sizes[i])));
			backingBuffer.position(backingBuffer.position()+sizes[i]);
		}
		
	}

	public Object get(String key) {
		materialize();
		Record r = records.get(key);
		if (r==null)
			return r;
		return r.value();
	}
	

	public ByteBuffer toByteBuffer() {
		if (records==null)
			return backingBuffer.rewind();
		LinkedHashMap<String, ByteBuffer> recordsOut =
				records
				.entrySet()
				.stream()
				.filter(e->!e.getValue().isNull())
				.collect(Collectors.toMap(e->e.getKey(),e->e.getValue().toByteBuffer(),(e1,e2)->e2,LinkedHashMap::new));
		
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
