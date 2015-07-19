package practice;

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface MyInstancable extends VersionedProtocol{
	long VERSION=465132L;
	public abstract String hello(String name);

	public abstract long getProtocolVersion(String protocol,long clientVersion)throws IOException;

}