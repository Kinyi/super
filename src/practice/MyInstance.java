package practice;

import java.io.IOException;

//import org.apache.hadoop.ipc.VersionedProtocol;

public class MyInstance implements MyInstancable{

	/* (non-Javadoc)
	 * @see RPC.MyInstancable#hello(java.lang.String)
	 */
	@Override
	public String hello(String name){
		System.out.println("我被调用了");
		return "hello"+name; 
	}

	/* (non-Javadoc)
	 * @see RPC.MyInstancable#getProtocolVersion(java.lang.String, long)
	 */
	@Override
	public long getProtocolVersion(String protocol,long clientVersion)throws IOException {
		// TODO Auto-generated method stub
		return VERSION;
	}
}
