package com.bigdata.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import com.bigdata.cassandra.practise.AppStarter;
import com.bigdata.cassandra.practise.models.Book;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.uuid.Uuids;

public class TestKeyspace {
	CqlSession session;
	String KEYSPACE_NAME = "test";
	AppStarter appT = new AppStarter();

	@Before
	public void connect() {
		session = CqlSession.builder()
				.withCloudSecureConnectBundle(Paths.get("/Users/harikagillela/course_work/secure-connect-dbtest.zip"))
				.withAuthCredentials("harikag", "Astra@1318").withKeyspace("test").build();
	}

	@Test
	public void whenCreatingAKeyspace_thenCreated() {
		// check if the schema is available
		String keyspaceName = "test";
		ResultSet result = session.execute("SELECT * FROM system_schema.keyspaces;");

		List<String> matchedKeyspaces = result.all().stream()
				.filter(r -> r.getString(0).equals(keyspaceName.toLowerCase())).map(r -> r.getString(0))
				.collect(Collectors.toList());

		assertEquals(1, matchedKeyspaces.size());
		assertTrue(matchedKeyspaces.get(0).equals(keyspaceName.toLowerCase()));
	}

	@Test
	public void whenCreatingATable_thenCreatedCorrectly() {

		appT.createTable();

		ResultSet result = session.execute("SELECT * FROM " + KEYSPACE_NAME + ".books;");
		List<String> columnNames = new ArrayList<>();

		result.getColumnDefinitions().forEach(cl -> {
			System.out.println(cl.getName().asInternal());
			columnNames.add(cl.getName().asInternal());
		});

		assertEquals(columnNames.size(), 3);
		assertTrue(columnNames.contains("id"));
		assertTrue(columnNames.contains("title"));
		assertTrue(columnNames.contains("subject"));
	}

	@Test
	public void whenAlteringTable_thenAddedColumnExists() {
		appT.createTable();
		appT.alterTablebooks("publisher", "text");

		ResultSet result = session.execute("SELECT * FROM " + KEYSPACE_NAME + "." + "books" + ";");

		List<String> columnNames = new ArrayList<>();

		result.getColumnDefinitions().forEach(cl -> {
			System.out.println(cl.getName().asInternal());
			columnNames.add(cl.getName().asInternal());
		});
		boolean columnExists = columnNames.contains("publisher");
		assertTrue(columnExists);
	}
	
	@Test
	public void whenSelectingAll_thenReturnAllRecords() {
		appT.createTable();
	        
	    Book book = new Book(
	    		Uuids.timeBased(), "Effective Java", "Programming");
	    appT.insertbookByTitle(book);
	      
	    book = new Book(
	    		Uuids.timeBased(), "Clean Code", "Programming");
	    appT.insertbookByTitle(book);
	        
	    List<Book> books = appT.selectAll(); 
	        
	    assertEquals(2, books.size());
	    assertTrue(books.stream().anyMatch(b -> b.getTitle()
	      .equals("Effective Java")));
	    assertTrue(books.stream().anyMatch(b -> b.getTitle()
	      .equals("Clean Code")));
	}
	
	
}
