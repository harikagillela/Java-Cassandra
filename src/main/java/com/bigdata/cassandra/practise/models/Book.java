package com.bigdata.cassandra.practise.models;

import java.util.UUID;

import com.datastax.oss.driver.api.core.uuid.Uuids;


public class Book {

	private UUID id;
	private String title;
	private String publisher;
	private String subject;
	public Book(UUID uuid, String title, String subject) {
		this.id = uuid;
		this.title = title;
		this.subject = subject;
	}
	public Book() {}
	
	public UUID getId() {
		return id;
	}
	public void setId(UUID id) {
		this.id = id;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public String getPublisher() {
		return publisher;
	}
	public void setPublisher(String publisher) {
		this.publisher = publisher;
	}
	public String getSubject() {
		return subject;
	}
	public void setSubject(String subject) {
		this.subject = subject;
	}
	@Override
	public String toString() {
		return "Book [id=" + id + ", title=" + title + ", publisher=" + publisher + ", subject=" + subject + "]";
	}
	
	
	
}
