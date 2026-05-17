package Topics.CAS;


// you can use join or synchronize method to get the correct count
public class SharedResourceUsingJoin {
    int counter;
    public void increment(){
        System.out.println(Thread.currentThread().getName());
        counter++;
    }
    public int get(){
        return counter;
    }
}

class JoinMain{
    public static void main(String[] args) {
        SharedResourceUsingJoin resource = new SharedResourceUsingJoin();

        Thread thread1 = new Thread(() -> {
            for(int i=0;i<400;i++){
                resource.increment();
            }
        });
        Thread thread2 = new Thread( () -> {
            for(int i=0;i<400;i++){
                resource.increment();
            }
        });

        thread1.start();
        thread2.start();

        try{
            thread1.join();
            thread2.join();
        }catch (Exception e){
            e.printStackTrace();
        }

        System.out.println("counter value : " + resource.get());
    }
}
