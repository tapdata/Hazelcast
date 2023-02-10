package com.hazelcast.persistence.http.entity;

import com.hazelcast.persistence.StringCompression;
import com.hazelcast.persistence.http.ObjectSerializerImpl;

import java.io.Serializable;
import java.util.Base64;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2022-10-19 11:38
 **/
public class IMapEntity implements Serializable {
	private static final long serialVersionUID = -2636615407534079903L;
	private String imap;
	private String key;
	private Map<String, Object> value;

	private IMapEntity() {
	}

	public static IMapEntity create() {
		return new IMapEntity();
	}

	public IMapEntity imap(String imap) {
		this.imap = imap;
		return this;
	}

	public IMapEntity key(String key) {
		this.key = key;
		return this;
	}

	public IMapEntity value(Map<String, Object> value) {
		this.value = value;
		return this;
	}

	public String getImap() {
		return imap;
	}

	public String getKey() {
		return key;
	}

	public Map<String, Object> getValue() {
		return value;
	}

	public Object getData() {
		if (null == value) return null;
		Object valueObj = value.get("value");
		if (valueObj instanceof String) {
			try {
				String uncompress = StringCompression.uncompress(valueObj.toString());
				return ObjectSerializerImpl.from(uncompress);
			} catch (Throwable ignored) {
				return valueObj;
			}
		} else {
			return valueObj;
		}
	}

	public IMapEntity serializeAndCompressValue() {
		if (null == value) return this;
		Object valueObj = value.get("value");
		try {
			byte[] bytes = ObjectSerializerImpl.to(valueObj);
			String encodeToString = Base64.getEncoder().encodeToString(bytes);
			String compress = StringCompression.compress(encodeToString);
			value.put("value", compress);
		} catch (Throwable ignored) {
		}
		return this;
	}

	@Override
	public String toString() {
		return "IMapEntity{" +
				"imap='" + imap + '\'' +
				", key='" + key + '\'' +
				", value=" + value +
				'}';
	}
}
