/**
 * @author yuanxiaokang
 * data 2020/9/26
 * 描述：
 */
public class Test {
    private String name;
    private int age;

    public Test(String name, int age){
        this.name = name;
        this.age = age;
    }

    protected void get(){
        System.out.println(age+"岁的"+name +"去游泳");
    }
}
