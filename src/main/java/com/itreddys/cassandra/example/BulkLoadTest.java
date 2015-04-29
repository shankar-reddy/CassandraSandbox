package com.itreddys.cassandra.example;

import com.datastax.driver.core.Session;


public class BulkLoadTest {
	
	private static final int LOAD_SIZE = 10000000;
	private static Session session = null;
	public static void main(String[] args) {
		session = CassandraClient.connect("172.16.1.51");
		BulkLoadTest bulkLoadTest = new BulkLoadTest();
		bulkLoadTest.truncateTable();
		bulkLoadTest.createKeySpace();
		bulkLoadTest.createTable();
		bulkLoadTest.loadData();
		
	}

	private void truncateTable() {
		System.out.println("Truncate table ...");
		session.execute("truncate bulkloadkp.bulktable ");
		System.out.println("... Done.");
		
	}

	/**
	 * Create Key Space if not exists
	 */
	public void createKeySpace() {
		System.out.println("Creating Cluster if not exists  ...");
		session.execute("CREATE KEYSPACE IF NOT EXISTS bulkloadkp WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};");
		System.out.println("... Done.");
	}

	/**
	 * Create Table if not exists
	 */
	public void createTable() {
		System.out.println("Creating Table if not exists  ...");
		// Create table Songs.
		session.execute(
				"CREATE TABLE IF NOT EXISTS bulkloadkp.bulktable (" +
						"id uuid PRIMARY KEY," + 
						"firstname text," + 
						"middlename text," + 
						"lastname text" + 
				");");

		System.out.println("Done.");
	}
	
	/**
	 * Load Data to tables
	 */
	public void loadData() {
		
		// Insert to Songs table
		long startTime = System.currentTimeMillis();
		for(int i = 0; i< LOAD_SIZE; i++){
			session.execute(
					"INSERT INTO bulkloadkp.bulktable (id, firstname, middlename, lastname) " +
							"VALUES (" +
							"uuid()," +
							"'Sankara_FirstNAME"+i+"'," +
							"'Reddy_MiddleNAME"+i+"'," +
							"'Telukutla_LastNAME"+i+"'" +
					");");
			
		}
		
		long endTime = System.currentTimeMillis();
		
		long totalTime = (endTime - startTime)/1000 ; //secs
		System.out.println("Total minutes : " +totalTime/60);
		
//		session.execute(
//				"INSERT INTO bulkloadkp.bulktable (id, firstname, middlename, lastname) " +
//						"VALUES (" +
//						"uuid()," +
//						"'sdfsdf'," +
//						"'La Petite Tonkinoise'," +
//						"'Joséphine Baker'" +
//				");");
		
		
		
	}

}
