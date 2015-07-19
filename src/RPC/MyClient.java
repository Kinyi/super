package RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.VersionedProtocol;

public class MyClient {

	
	public static void main(String[] args) throws IOException {
		final MyBizable proxy = (MyBizable)RPC.waitForProxy(//Class<? extends VersionedProtocol> protocol
					MyBizable.class,
					MyBizable.VERSION,
					new InetSocketAddress(MyServer.ADDRESS,MyServer.PORT),
					new Configuration());
		String result=proxy.hello("world!");
		System.out.println(result);
		RPC.stopProxy(proxy);
	}

}
