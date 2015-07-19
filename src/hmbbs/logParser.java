//this is a test
package hmbbs;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class logParser {

	/**
	 * @param args
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws ParseException {
		String data = "27.19.74.143 - - [30/May/2013:17:38:20 +0800] \"GET /static/image/common/faq.gif HTTP/1.1\" 200 1127";
		final logParser logParser = new logParser();
		final String[] parse = logParser.parse(data);
		System.out.println(parse[0]+" "+parse[1]+" "+parse[2]);
	}
	/*public static final SimpleDateFormat FORMAT = new SimpleDateFormat("d/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
	public static final SimpleDateFormat dateformat1=new SimpleDateFormat("yyyyMMddHHmmss");*/
	public String[] parse(String line) throws ParseException{
		String ip = this.parseIp(line);
		String date = this.parseDate(line);
		String url = this.parseUrl(line);
		return new String[]{ip,date,url};
	}
	/*private Date parseDateFormat(String string){
		Date parse = null;
		try {
			parse = FORMAT.parse(string);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return parse;
	}*/
	public String parseIp(String line){
		final String ip = line.split("- -")[0].trim();
		return ip;
	}
	public String parseDate(String line) throws ParseException{
		final int first = line.indexOf("[");
		final int last = line.lastIndexOf("+0800]");
		final String time = line.substring(first+1, last).trim();
		String date = "";
		final SimpleDateFormat parseDate = new SimpleDateFormat("dd/MMMM/yyyy:HH:mm:ss",Locale.ENGLISH);
		final Date parse = parseDate.parse(time);
		final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		date = simpleDateFormat.format(parse);
		return date;
	}
	/*private String parseTime(String line) {
		final int first = line.indexOf("[");
		final int last = line.indexOf("+0800]");
		String time = line.substring(first+1,last).trim();
		Date date = parseDateFormat(time);
		return dateformat1.format(date);
	}*/
	public String parseUrl(String line){
		final String[] split = line.split(" ");
		return split[6];
	}

}
