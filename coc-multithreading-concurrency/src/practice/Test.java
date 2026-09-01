package practice;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Test {

    public static void main(String[] args) {
        ExecutorService service = Executors.newFixedThreadPool(3);
        Printer printer = new Printer();
        service.submit(() -> printer.print(1));
        service.submit(() -> printer.print(2));
        service.submit(() -> printer.print(3));

        service.shutdown();
    }
}

class Printer{

    private final int MAX_COUNT = 15;
    private int currentCount = 1;

    Lock lock = new ReentrantLock();
    Condition condition = lock.newCondition();

    public void print(int threadId){
        while (currentCount < MAX_COUNT){
            lock.lock();
            try{
                while (currentCount%3 != threadId%3 && currentCount < MAX_COUNT){
                    condition.await();
                }

                if(currentCount >= MAX_COUNT){
                    condition.signalAll();
                    break;
                }

                System.out.println(currentCount + " " + Thread.currentThread().getName());
                currentCount++;
                condition.signal();
            }catch(InterruptedException e){
                Thread.currentThread().interrupt();
                return;
            }finally {
                lock.unlock();
            }
        }
    }
}

