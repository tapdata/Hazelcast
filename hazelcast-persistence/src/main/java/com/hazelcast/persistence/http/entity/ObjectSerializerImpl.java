package com.hazelcast.persistence.http.entity;

import java.io.*;

/**
 * @author samuel
 * @Description
 * @create 2022-10-19 17:25
 **/
public class ObjectSerializerImpl {
	public static byte[] to(Object obj) throws IOException {
		if (null == obj) return null;
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
		objectOutputStream.writeObject(obj);
		return byteArrayOutputStream.toByteArray();
	}

	public static Object from(byte[] bytes) throws IOException, ClassNotFoundException {
		if (null == bytes) return null;
		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
		ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
		return objectInputStream.readObject();
	}
}
