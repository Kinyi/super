package practice;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

public class MyServer {

	/**
	 * @param args
	 */
	static final String ADDRESS="localhost";
	static final int PORT=1324;
	public static void main(String[] args) throws Exception {
		/** Construct an RPC server.             
	     * @param instance the instance whose methods will be called
	     * @param conf the configuration to use
	     * @param bindAddress the address to bind on to listen for connection
	     * @param port the port to listen for connections on
	     * @param numHandlers the number of method handler threads to run
	     * @param verbose whether each call should be logged
	     */
		final Server server = RPC.getServer(new MyInstance(), ADDRESS, PORT, new Configuration());
		server.start();
	}

}
