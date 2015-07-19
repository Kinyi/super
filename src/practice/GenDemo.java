package practice;

class Gen<T> {
	private T ob; // ���巺�ͳ�Ա����

	public Gen(T ob) {
		this.ob = ob;
	}

	public T getOb() {
		return ob;
	}

	public void setOb(T ob) {
		this.ob = ob;
	}

	public void showType() {
		System.out.println("T��ʵ��������: " + ob.getClass().getName());
		System.out.println(ob.getClass());
		System.out.println(GenDemo.class.getName());
	}
}
	
public class GenDemo {
	public static void main(String[] args) {
		// ���巺����Gen��һ��Integer�汾
		Gen<Integer> intOb = new Gen<Integer>(88);
		intOb.showType();
		int i = intOb.getOb();
		System.out.println("value= " + i);
		System.out.println("----------------------------------");
		// ���巺����Gen��һ��String�汾
		Gen<String> strOb = new Gen<String>("Hello Gen!");
		strOb.showType();
		String s = strOb.getOb();
		System.out.println("value= " + s);
	}
}