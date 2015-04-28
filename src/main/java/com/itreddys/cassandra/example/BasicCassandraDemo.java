package com.itreddys.cassandra.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * This is Basic Example which covers Create KeySpace, create table, Insert Data and Query Data.
 * 
 * @author Sankara Reddy Telukutla
 *
 */
public class BasicCassandraDemo {

	private Cluster cluster;
	private Session session;


	/**
	 * Run from here.
	 * @param args
	 */
	public static void main(String[] args) {
		BasicCassandraDemo example = new BasicCassandraDemo();
		
		String nodeIP = "172.16.1.51";
		example.connect(nodeIP);
		example.listKeySpace();
		example.createKeySpace();
		example.createTable();
		example.querySchema();
		example.loadData();
	}
	

	private void listKeySpace() {
		ResultSet results = session.execute("SELECT * FROM system.schema_keyspaces;");
		for (Row row : results) {
			System.out.println(row.getString("keyspace_name"));
			
		}
		
	}


	/**
	 * Connect to Cassandra 
	 * @param node
	 */
	public void connect(String node){
		this.session = CassandraClient.connect(node);
	}

	/**
	 * Get Session
	 * @return
	 */
	public Session getSession() {
		return this.session;
	}

	/**
	 * Recycle things.
	 */
	public void close() {
		session.close();
		cluster.close();
	}

	/**
	 * Create Key Space if not exists
	 */
	public void createKeySpace() {
		System.out.println("Creating Cluster if not exists  ...");
		session.execute("CREATE KEYSPACE IF NOT EXISTS itreddys WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};");
		System.out.println("... Done.");
	}

	
	/**
	 * Create Table if not exists
	 */
	public void createTable() {
		System.out.println("Creating Table if not exists  ...");
		// Create table Songs.
		session.execute(
				"CREATE TABLE IF NOT EXISTS itreddys.songs (" +
						"id uuid PRIMARY KEY," + 
						"title text," + 
						"album text," + 
						"artist text," + 
						"tags set<text>," + 
						"data blob" + 
				");");
		
		// Create table playlists.
		session.execute(
				"CREATE TABLE IF NOT EXISTS itreddys.playlists (" +
						"id uuid," +
						"title text," +
						"album text, " + 
						"artist text," +
						"song_id uuid," +
						"PRIMARY KEY (id, title, album, artist)" +
				");");
		
		System.out.println("Done.");
	}
	
	/**
	 * Load Data to tables
	 */
	public void loadData() {
		
		// Insert to Songs table
		session.execute(
				"INSERT INTO itreddys.songs (id, title, album, artist, tags) " +
						"VALUES (" +
						"756716f7-2e54-4715-9f00-91dcbea6cf50," +
						"'La Petite Tonkinoise'," +
						"'Bye Bye Blackbird'," +
						"'Joséphine Baker'," +
						"{'jazz', '2013'})" +
				";");
		
		// Insert to playlists table.
		session.execute(
				"INSERT INTO itreddys.playlists (id, song_id, title, album, artist) " +
						"VALUES (" +
						"2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
						"756716f7-2e54-4715-9f00-91dcbea6cf50," +
						"'La Petite Tonkinoise'," +
						"'Bye Bye Blackbird'," +
						"'Joséphine Baker'" +
				");");
	}


	/**
	 * Query Schema
	 */
	public void querySchema() {
		
		//Search 
		ResultSet results = session.execute("SELECT * FROM itreddys.playlists WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
		System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",	"-------------------------------+-----------------------+--------------------"));
		for (Row row : results) {
			System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"),	row.getString("album"),  row.getString("artist")));
		}
		System.out.println();
	}
}
