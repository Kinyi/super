package RPC;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface MyBizable extends VersionedProtocol{
	long VERSION=466456L;
	public abstract String hello(String name);

}