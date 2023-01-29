package simpledb.transaction;

import simpledb.storage.PageId;

/**
 * @author kevin.zeng
 * @description
 * @create 2023-01-23
 */
public class Lock {
    private TransactionId tid;
    private PageId pid;
    private LockType type;

    public Lock(TransactionId tid, PageId pid, LockType type) {
        this.tid = tid;
        this.type = type;
        this.pid = pid;
    }

    public TransactionId getTid() {
        return tid;
    }

    public void setTid(TransactionId tid) {
        this.tid = tid;
    }

    public LockType getType() {
        return type;
    }

    public void setType(LockType type) {
        this.type = type;
    }

    public PageId getPid() {
        return pid;
    }

    public void setPid(PageId pid) {
        this.pid = pid;
    }
}
