package CAS;

public class CASSharedResource {

    int counter;
    public void increment(){
        counter++;
    }
    public int get(){
        return counter;
    }
}

class CASMain{
    public static void main(String[] args) {
        CASSharedResource resource = new CASSharedResource();
        for(int i=0;i<400;i++){
            resource.increment();
        }
        System.out.println(resource.get());
    }
}
