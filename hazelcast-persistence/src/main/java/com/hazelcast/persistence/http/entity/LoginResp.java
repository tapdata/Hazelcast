package com.hazelcast.persistence.http.entity;

import org.apache.commons.lang3.StringUtils;

import java.time.Instant;

/**
 * @author samuel
 * @Description
 * @create 2022-10-19 11:41
 **/
public class LoginResp {
	private String id;

	private String created;

	private String userId;

	private Long ttl;

	private long expiredTimestamp;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getCreated() {
		return created;
	}

	public void setCreated(String created) {
		this.created = created;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public Long getTtl() {
		return ttl;
	}

	public void setTtl(Long ttl) {
		this.ttl = ttl;
	}

	public long getExpiredTimestamp() {
		return expiredTimestamp;
	}

	public void setExpiredTimestamp(long expiredTimestamp) {
		this.expiredTimestamp = expiredTimestamp;
	}

	public void calcExpiredTimestamp() {
		if (StringUtils.isBlank(created) || ttl.compareTo(0L) <= 0) {
			return;
		}
		Instant instant = Instant.parse(created);
		long ts = instant.toEpochMilli();
		this.expiredTimestamp = ts + (ttl * 1000);
	}
}
