package com.itreddys.cassandra.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

/**
 * 
 * @author Sankara Telukutla
 *
 */
public class CassandraClient {
	private static Cluster cluster;
	private static String nodeIP = "127.0.0.1";

	public static void main(String[] args) {
		CassandraClient client = new CassandraClient();
		client.connect(nodeIP);
		client.close();
	}

	/**
	 * Provide the Node to connect
	 * 
	 * @param node
	 * @return 
	 */
	public  static Session connect(String node) {
		if(node == null) node = nodeIP;
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n",
				metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}
		return cluster.connect();
	}
	

	/**
	 * Close the connection
	 */
	public void close() {
		cluster.close();
	}

}
