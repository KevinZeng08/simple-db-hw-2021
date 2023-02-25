package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author kevin.zeng
 * @description
 * @create 2023-01-23
 */
public class LockManager {
    private ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, PageLock>> lockMap;

//    private Map<TransactionId, Set<PageId>>

    public LockManager() {
        this.lockMap = new ConcurrentHashMap<>();
    }

    // only one thread can acquire lock each time
    public synchronized boolean acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {

            int requiredType = perm == Permissions.READ_ONLY ? PageLock.SHARED : PageLock.EXCLUSIVE;
            final String thread = Thread.currentThread().getName();
            if(lockMap.get(pid) == null){
                PageLock pageLock = new PageLock(tid,requiredType);
                ConcurrentHashMap<TransactionId,PageLock> pageLocks = new ConcurrentHashMap<>();
                pageLocks.put(tid,pageLock);
                lockMap.put(pid,pageLocks);
                //System.out.println(thread + ": the " + pageId + " have no lock, transaction" + tid + " require " + lockType + ", accept");
                return true;
            }
            ConcurrentHashMap<TransactionId,PageLock> pageLocks = lockMap.get(pid);

            if(pageLocks.get(tid) == null){
                // tid没有该page上的锁
                if(pageLocks.size() > 1){
                    //page 上有其他事务的读锁
                    if (requiredType == PageLock.SHARED){
                        //tid 请求读锁
                        PageLock pageLock = new PageLock(tid,PageLock.SHARED);
                        pageLocks.put(tid,pageLock);
                        lockMap.put(pid,pageLocks);
                        //System.out.println(thread + ": the " + pageId + " have many read locks, transaction" + tid + " require " + lockType + ", accept and add a new read lock");
                        return true;
                    }
                    if (requiredType == PageLock.EXCLUSIVE){
                        // tid 需要获取写锁
//                        wait(20);
//                        System.out.println(thread + ": the " + pid + " have lock with diff txid, transaction" + tid + " require write lock, await...");
                        return false;
                    }
                }
                if (pageLocks.size() == 1){
                    //page 上有一个其他事务的锁  可能是读锁，也可能是写锁
                    PageLock curLock = null;
                    for (PageLock lock : pageLocks.values()){
                        curLock = lock;
                    }
                    if (curLock.getType() == PageLock.SHARED){
                        //如果是读锁
                        if (requiredType == PageLock.SHARED){
                            // tid 需要获取的是读锁
                            PageLock pageLock = new PageLock(tid,PageLock.SHARED);
                            pageLocks.put(tid,pageLock);
                            lockMap.put(pid,pageLocks);
                            //System.out.println(thread + ": the " + pageId + " have one read lock with diff txid, transaction" + tid + " require read lock, accept and add a new read lock");
                            return true;
                        }
                        if (requiredType == PageLock.EXCLUSIVE){
                            // tid 需要获取写锁
//                            wait(10);
//                            System.out.println(thread + ": the " + pid + " have lock with diff txid, transaction" + tid + " require write lock, await...");
                            return false;
                        }
                    }
                    if (curLock.getType() == PageLock.EXCLUSIVE){
                        // 如果是写锁
//                        wait(10);
//                        System.out.println(thread + ": the " + pid + " have one write lock with diff txid, transaction" + tid + " require read lock, await...");
                        return false;
                    }
                }

            }
            if (pageLocks.get(tid) != null){
                // tid有该page上的锁
                PageLock pageLock = pageLocks.get(tid);
                if (pageLock.getType() == PageLock.SHARED){
                    // tid 有 page 上的读锁
                    if (requiredType == PageLock.SHARED){
                        //tid 需要获取的是读锁
                        //System.out.println(thread + ": the " + pageId + " have one lock with same txid, transaction" + tid + " require " + lockType + ", accept");
                        return true;
                    }
                    if (requiredType == PageLock.EXCLUSIVE){
                        //tid 需要获取的是写锁
                        if(pageLocks.size() == 1){
                            // 该page上 只有tid的 读锁，则可以将其升级为写锁
                            pageLock.setType(PageLock.EXCLUSIVE);
                            pageLocks.put(tid,pageLock);
                            //System.out.println(thread + ": the " + pageId + " have read lock with same txid, transaction" + tid + " require write lock, accept and upgrade!!!");
                            return true;
                        }
                        if (pageLocks.size() > 1){
                            // 该page 上还有其他事务的读锁，则不能升级
//                            System.out.println(thread + ": the " + pid + " have many read locks, transaction" + tid + " require write lock, abort!!!");
//                            return false;
                            throw new TransactionAbortedException();
                        }
                    }
                }
                if (pageLock.getType() == PageLock.EXCLUSIVE){
                    // tid 有 page上的写锁
                    //System.out.println(thread + ": the " + pageId + " have write lock with same txid, transaction" + tid + " require " + lockType + ", accept");
                    return true;
                }
            }

            System.out.println("----------------------------------------------------");
            return false;
    }

    public synchronized boolean releaseLock(PageId pid) {
        lockMap.remove(pid);
        return true;
    }

    public synchronized boolean releaseLock(TransactionId tid, PageId pid) {
        if(hasLock(tid,pid)) {
            ConcurrentHashMap<TransactionId, PageLock> pageLocks = lockMap.get(pid);
            pageLocks.remove(tid);
            if(pageLocks.size() == 0) {
                lockMap.remove(pid);
            }
            // notify other threads
            this.notifyAll();
            return true;
        }
        return false;
    }

    public synchronized boolean hasLock(TransactionId tid, PageId pid) {
        ConcurrentHashMap<TransactionId, PageLock> pageLocks = lockMap.get(pid);
        if (pageLocks == null) {
            return false;
        }
        PageLock pageLock = pageLocks.get(tid);
        if(pageLock == null) return false;
        return true;
    }

    public synchronized void completeTransaction(TransactionId tid){
        // release all locks of tid
        Set<PageId> pageIds = lockMap.keySet();
        for(PageId pid : pageIds) {
            releaseLock(tid, pid);
        }
    }
}
