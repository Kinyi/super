package practice;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;

public class App2 {
	static final String PATH= "hdfs://chaoren:9000/";
	static final String FILE= "/d1/hello";
	static final String DIR= "/d1";
	public static void main(String[] args) throws Exception {
		FileSystem fileSystem=getFileSystem();
	    //�����ļ���
		mkdir(fileSystem);	
		//�ϴ��ļ�
		putData(fileSystem);
		//�����ļ�
		getData(fileSystem);
		//����ļ���
		list(fileSystem);
		//ɾ���ļ���
		remove(fileSystem);
	}
	public static void remove(FileSystem fileSystem)throws IOException {
		fileSystem.delete(new Path(DIR), true);
	}
	public static void list(FileSystem fileSystem)throws IOException {
		final FileStatus[] listStatus = fileSystem.listStatus(new Path(DIR));
		for (FileStatus fileStatus : listStatus) {
			final String isdir = fileStatus.isDir()? "�ļ���":"�ļ�";
			final short replication = fileStatus.getReplication();
			final FsPermission permission = fileStatus.getPermission();
			final Path path = fileStatus.getPath();
			final long len = fileStatus.getLen();
			System.out.println(isdir+"\t"+replication+"\t"+permission+"\t"+path+"\t"+len);
		}
	}
	public static void getData(FileSystem fileSystem)throws IOException {
		final FSDataInputStream in = fileSystem.open(new Path(FILE));
		IOUtils.copyBytes(in, System.out, 1024, true);
	}
	public static void putData(FileSystem fileSystem)throws FileNotFoundException,IOException {
		final FileInputStream in = new FileInputStream("E://martin/practice.txt");
		final FSDataOutputStream out = fileSystem.create(new Path(FILE));
		IOUtils.copyBytes(in, out, 1024, true);
	}
	public static void mkdir(FileSystem fileSystem)throws IOException {
		fileSystem.mkdirs(new Path(PATH));
	}
	public static FileSystem getFileSystem()throws IOException,URISyntaxException {
		return FileSystem.get(new URI(PATH), new Configuration());
	}
}
