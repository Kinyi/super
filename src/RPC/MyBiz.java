package RPC;

import java.io.IOException;

public class MyBiz implements MyBizable {

	/* (non-Javadoc)
	 * @see RPC.MyBizable#hello(java.lang.String)
	 */
	@Override
	public String hello(String name){
		System.out.println("�ұ�������");
		return "hello "+name;
	}

	@Override
	public long getProtocolVersion(String protocol,long clientVersion)throws IOException {
		// TODO Auto-generated method stub
		return VERSION;
	}

}
