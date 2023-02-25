package simpledb.transaction;

/**
 * @author kevin.zeng
 * @description
 * @create 2023-02-23
 */
public class PageLock {
    public static final int SHARED = 1;
    public static final int EXCLUSIVE = 2;
    private TransactionId tid;
    private int type;

    public PageLock(TransactionId tid, int type) {
        this.tid = tid;
        this.type = type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public TransactionId getTid() {
        return tid;
    }
}
