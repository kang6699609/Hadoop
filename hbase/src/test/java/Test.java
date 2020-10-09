/**
 * @author yuanxiaokang
 * data 2020/9/28
 * 描述：
 */
public class Test {
    {
        System.out.println("AABBCC");
    }
    static {
        System.out.println("AABBCC");
    }

    public static void main(String[] args) {
        Test test = new Test();
    }
}
