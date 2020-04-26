package core;

public class ConstructorT<T,S>{

    private T certNo;

    private T name;

    private T mobile;

    private S address;

    public ConstructorT(){
        super();
    }
    public ConstructorT(T certNo){
        this.certNo = certNo;
    }
    public ConstructorT(T certNo, T name){
        this.certNo = certNo;
        this.name = name;
    }
    public ConstructorT(T certNo, T name, T mobile){
        this.certNo = certNo;
        this.name = name;
        this.mobile = mobile;
    }
    public ConstructorT(T certNo, T name, T mobile, S address){
        this.certNo = certNo;
        this.name = name;
        this.mobile = mobile;
        this.address = address;
    }
    public T getCertNo(){
        return this.certNo;
    }

    public static void main(String[] args) {
        ConstructorT<String, String> constructorT =  new ConstructorT<>("123456");
        System.out.println(constructorT.getCertNo());
    }
}
