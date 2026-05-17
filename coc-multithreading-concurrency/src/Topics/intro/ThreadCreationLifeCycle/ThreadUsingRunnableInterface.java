package Topics.intro.ThreadCreationLifeCycle;

public class ThreadUsingRunnableInterface implements Runnable{
    @Override
    public void run(){
        System.out.println("code executed by thread: " + Thread.currentThread().getName());
    }
}

class ThreadUsingRunnableInterfaceMain{
    public static void main(String[] args) {
        System.out.println("main method: " + Thread.currentThread().getName());

        ThreadUsingRunnableInterface obj = new ThreadUsingRunnableInterface();
        Thread thread1 = new Thread(obj);
        thread1.start();
//        thread1.run();
//        Note: If you had called thread1.run() directly instead of thread1.start(), no new thread would be created.
//        The code would simply run on the main thread like a normal method call.

        System.out.println("finish main method: " + Thread.currentThread().getName());
    }
}
