package Topics.intro.ThreadCreationLifeCycle;

public class ThreadUsingThreadClass extends Thread{

    @Override
    public void run(){
        System.out.println("code executed by thread: " + Thread.currentThread().getName());
    }
}

class ThreadUsingThreadClassMain{
    public static void main(String[] args) {
        System.out.println("main method: " + Thread.currentThread().getName());

        ThreadUsingThreadClass thread1 = new ThreadUsingThreadClass();
        thread1.start();

        System.out.println("finish main method: " + Thread.currentThread().getName());
    }
}