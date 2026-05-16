package LocksAndConditions.ReadWriteLock;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class SharedResource{
    boolean isAvailable = false;
    public void producer(ReadWriteLock lock){
        try{
            lock.readLock().lock();
            System.out.println("Read lock acquired by : " + Thread.currentThread().getName());
            Thread.sleep(2000);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            lock.readLock().unlock();
            System.out.println("Read lock release by : " + Thread.currentThread().getName());
        }
    }

    public void consumer(ReadWriteLock lock){
        try {
            lock.writeLock().lock();
            System.out.println("Write lock acquired by : " + Thread.currentThread().getName());
            isAvailable = false;
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            lock.writeLock().unlock();
            System.out.println("Write lock released by : " + Thread.currentThread().getName());
        }
    }
}

public class ReadWriteLockMain {
    public static void main(String[] args) {
        SharedResource resource = new SharedResource();
        ReadWriteLock lock = new ReentrantReadWriteLock();

        Thread thread1 = new Thread(() -> {
            resource.producer(lock);
        });

        Thread thread2 = new Thread(() -> {
            resource.producer(lock);
        });

        SharedResource resource1 = new SharedResource();
        Thread thread3 = new Thread(() -> {
            resource1.consumer(lock);
        });

        thread1.start();
        thread2.start();
        thread3.start();
    }
}
