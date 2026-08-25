package InterviewProblems;

public class SingletonDemo {

    private SingletonDemo (){

    }

    private static class DBConnection{
        private static final SingletonDemo INSTANCE_OBJ = new SingletonDemo();
    }

    private static SingletonDemo getInstance(){
        return DBConnection.INSTANCE_OBJ;
    }

    public static void main(String[] args) {
        EnumSingleton.INSTANCE.doSomething();
    }
}

enum EnumSingleton{
    INSTANCE;

    public void doSomething(){
        System.out.println("cool");
    }
}
