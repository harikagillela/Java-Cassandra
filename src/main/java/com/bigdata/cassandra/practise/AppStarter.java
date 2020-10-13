package com.bigdata.cassandra.practise;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.bigdata.cassandra.practise.models.Book;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.uuid.Uuids;


public class AppStarter {
	
	public static CqlSession session = null;
	//creating a column family -- books
	private static final String TABLE_NAME = "books";
	 
	
	public String createTable() {
		System.out.println("###### Running createTable() ######");
	    StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
	      .append(TABLE_NAME).append("(")
	      .append("id uuid PRIMARY KEY, ")
	      .append("title text,")
	      .append("subject text);");
	 //test
	    String query = sb.toString();
	    System.out.println(query);
	    session.execute(query);
	    System.out.println("###### Finished createTable() ######");
	    return(query);
	}
	//Alter the column family -- books
	public String alterTablebooks(String columnName, String columnType) {
		System.out.println("###### Running alterTablebooks() ######");
	    StringBuilder sb = new StringBuilder("ALTER TABLE ")
	      .append(TABLE_NAME).append(" ADD ")
	      .append(columnName).append(" ")
	      .append(columnType).append(";");
	 
	    String query = sb.toString();
	    System.out.println(query);
	    session.execute(query);
	    System.out.println("###### Finished alterTablebooks() ######");
	    return(query);
	}
	//Insert Data to the column family
	public String insertbookByTitle(Book book) {
		System.out.println("###### Running insertbookByTitle() ######");
		//validate book
		
	    StringBuilder sb = new StringBuilder("INSERT INTO ")
	      .append("books").append("(id, title, subject) ")
	      .append("VALUES (").append(book.getId())
	      .append(", '").append(book.getTitle())
	      .append("', '").append(book.getSubject())
	      .append("');");
	 
	    String query = sb.toString();
	    System.out.println(query);
	    session.execute(query);
	    System.out.println("###### Finished insertbookByTitle() ######");
	    return(query);
	}
	
	public String updatebookById(String id, String publisher) {
		System.out.println("###### Running updatebookByTitle() ######");
		//validate book
		
	    StringBuilder sb = new StringBuilder("UPDATE ")
	      .append("books").append(" SET publisher = ").append("'").append(publisher)
	      .append("'")
	      .append(" where id = ")
	      //.append("'")
	      .append(id)
	      .append(";");
	 //"UPDATE books SET subject = 'test' where title = '';"
	    String query = sb.toString();
	    System.out.println(query);
	    session.execute(query);
	    System.out.println("###### Finished updatebookByTitle() ######");
	    return(query);
	}
	
	private void deleteBookById(String id) {
		System.out.println("###### Running deleteBookById() ######");
		
	    StringBuilder sb = new StringBuilder("DELETE FROM ")
	      .append("books").append(" WHERE id = ").append(id)
	      .append(";");
	 //"DELETE from books where id  = ;"
	    String query = sb.toString();
	    System.out.println(query);
	    session.execute(query);
	    System.out.println("###### Finished deleteBookById() ######");
	}
	
	//Create a new column Family - booksByTitle
	public String createTableBooksByTitle() {
		System.out.println("###### Running createTableBooksByTitle() ######");
	    StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
	      .append("booksByTitle").append("(")
	      .append("id uuid, ")
	      .append("title text,")
	      .append("PRIMARY KEY (title, id));");
	 
	    String query = sb.toString();
	    System.out.println(query);
	    session.execute(query);
	    System.out.println("###### Finished createTableBooksByTitle() ######");
	    return(query);
	}
	
	
	
	//to satisfy selectByTitle, compound primary key
	//title is partitioning key, id column is clustering key 
	public List<Book> selectAll() {
		System.out.println("###### Running SelectAll() ######");
	    StringBuilder sb = 
	      new StringBuilder("SELECT * FROM ").append(TABLE_NAME);
	 
	    String query = sb.toString();
	    System.out.println(query);
	    ResultSet rs = session.execute(query);
	 
	    List<Book> books = new ArrayList<Book>();
	 
	    rs.forEach(r -> {
	    	Book book = new Book(
	  	          r.getUuid("id"), 
	  	          r.getString("title"),  
	  	          r.getString("subject"));
	    	System.out.println("Book - " + book);
	        books.add(book);
	    });
	    System.out.println("###### Finished SelectAll() ######");
	    return books;
	}
	
	public void insertBookBatch(Book book) {
		System.out.println("###### Running insertBookBatch() ######");
		
	    StringBuilder sb = new StringBuilder("BEGIN BATCH ")
	      .append("INSERT INTO ").append(TABLE_NAME)
	      .append("(id, title, subject) ")
	      .append("VALUES (").append(book.getId()).append(", '")
	      .append(book.getTitle()).append("', '")
	      .append(book.getSubject()).append("');")
	      .append("INSERT INTO ")
	      .append("booksByTitle").append("(id, title) ")
	      .append("VALUES (").append(book.getId()).append(", '")
	      .append(book.getTitle()).append("');")
	      .append("APPLY BATCH;");
	 
	    String query = sb.toString();
	    System.out.println(query);
	    session.execute(query);
	    System.out.println("###### Finished insertBookBatch() ######");
	}
	
	public void deleteTable() {
		System.out.println("###### Running deleteTable() ######");
	    StringBuilder sb = 
	      new StringBuilder("DROP TABLE IF EXISTS ").append(TABLE_NAME);
	 
	    String query = sb.toString();
	    System.out.println(query);
	    session.execute(query);
	    System.out.println("###### Finished deleteTable() ######");
	}
	
	public static void main(String[] args) {
		
		if(null!=session)
			session.close();
		
		session = CqlSession.builder()
				.withCloudSecureConnectBundle(Paths.get("/Users/harikagillela/course_work/secure-connect-dbtest.zip"))
				.withAuthCredentials("harikag", "Astra@1318")
				//keyspace -- replication strategy : simple strategy replication factor 1
				.withKeyspace("library").
				build();
		
		AppStarter appT = new AppStarter();
		//appT.createTable();
		//appT.alterTablebooks("publisher", "text");
		//appT.insertbookByTitle(new Book(Uuids.timeBased(), "Effective Java", "Encapsulation"));
		//appT.insertbookByTitle(new Book(Uuids.timeBased(), "Advance Java", "OOPS"));
		//appT.insertbookByTitle(new Book(Uuids.timeBased(), "BigData", "HDFS"));
		//appT.insertbookByTitle(new Book(Uuids.timeBased(), "Hive", "Query"));
		//appT.updatebookById("8d2c61e0-0382-11eb-a233-5f36f280cb5b", "TGH");
		//appT.updatebookById("8d3d2ac0-0382-11eb-a233-5f36f280cb5b", "TGH");
		//appT.deleteBookById("8d3d2ac0-0382-11eb-a233-5f36f280cb5b");
		//appT.createTableBooksByTitle();
		//appT.selectAll();
		appT.deleteTable();	
		session.close();
			
		System.exit(0);
	}
	

}
