package practice;


import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Test {

}


class PrintNumbers{
    private static final int MAX_NUMBERS = 10;
    private static final int TOTAL_THREADS = 3;
    private int currentNum = 1;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition stateChanged = lock.newCondition();

    private void printNumber(int threadId){
        while (true){
            lock.lock();
            try {
                while (currentNum <= MAX_NUMBERS && currentNum % TOTAL_THREADS != threadId % TOTAL_THREADS){
                    stateChanged.await();
                }
                if(currentNum > MAX_NUMBERS){
                    stateChanged.signalAll();
                    break;
                }
                currentNum++;
                stateChanged.signalAll();
            }catch (InterruptedException e){
                break;
            }finally {
                lock.unlock();
            }
        }
    }

    public static void main(String[] args) {
        PrintNumbers printNumbers = new PrintNumbers();
        ExecutorService executorService = Executors.newFixedThreadPool(TOTAL_THREADS);
        for(int i=1;i<=TOTAL_THREADS;i++){
            final int threadId = i;
            executorService.submit(() -> printNumbers.printNumber(threadId));
        }
        executorService.shutdown();
    }

}


