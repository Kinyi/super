package practice;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.VersionedProtocol;

public class MyClient {

	public static void main(String[] args) throws Exception{
		MyInstancable proxy = (MyInstancable)RPC.waitForProxy(//Class<? extends VersionedProtocol> protocol,
				MyInstancable.class,
			      MyInstancable.VERSION,
			      new InetSocketAddress(MyServer.ADDRESS,MyServer.PORT),
			      new Configuration());
		final String result = proxy.hello(" world!");
		System.out.println("客户端结果："+result);
		RPC.stopProxy(proxy);
	}
	
}
