package com.hazelcast.persistence.http.entity;

import java.io.Serializable;
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

	@Override
	public String toString() {
		return "IMapEntity{" +
				"imap='" + imap + '\'' +
				", key='" + key + '\'' +
				", value=" + value +
				'}';
	}
}
