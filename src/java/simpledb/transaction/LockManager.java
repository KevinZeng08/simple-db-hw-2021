package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.*;

/**
 * @author kevin.zeng
 * @description
 * @create 2023-01-23
 */
public class LockManager {
    private Map<PageId, List<Lock>> locksPerPage;

//    private Map<TransactionId, Set<PageId>>

    public LockManager() {
        this.locksPerPage = new HashMap<>();
    }

    // only one thread can acquire lock each time
    public synchronized boolean acquireLock(TransactionId tid, PageId pid, Permissions perm) {
        // Only one transaction may have an exclusive lock on an object
        TransactionId owner = getOwner(pid);
        if(owner != null) {
            if(owner.equals(tid)) return true;
            else return false;
        }
        if (perm == Permissions.READ_ONLY) {
            // modify locks per Page
            if (locksPerPage.containsKey(pid)) {
                // check whether shared lock of this transaction exists
                for(Lock lock : locksPerPage.get(pid)) {
                    if(lock.getTid().equals(tid)) return true;
                }
                // acquire shared lock
                Lock sharedLock = new Lock(tid, pid, LockType.SHARED);
                locksPerPage.get(pid).add(sharedLock);
            } else {
                // acquire shared lock
                Lock sharedLock = new Lock(tid, pid, LockType.SHARED);
                List<Lock> newLocks = new ArrayList<>();
                newLocks.add(sharedLock);
                locksPerPage.put(pid, newLocks);
            }
            return true;
        } else if (perm == Permissions.READ_WRITE) {
            // acquire exclusive lock
            if (locksPerPage.containsKey(pid)) {
                List<Lock> locks = locksPerPage.get(pid);
                if (locks.size() > 1) {
                    return false;
                }
                if (locks.isEmpty()) {
                    Lock exclusiveLock = new Lock(tid, pid, LockType.EXCLUSIVE);
                    locks.add(exclusiveLock);
                    locksPerPage.put(pid, locks);
                    return true;
                }
                // if shared lock of Tx is the only lock of this Page, upgrade to exclusive lock
                Lock lock = locks.get(0);
                if (lock.getTid().equals(tid)) {
                    // upgrade
                    locks.get(0).setType(LockType.EXCLUSIVE);
                    locksPerPage.put(pid, locks);
                    return true;
                } else {
                    // shared lock of other Transaction exists
                    return false;
                }
            } else {
                List<Lock> newLocks = new ArrayList<>();
                Lock exclusiveLock = new Lock(tid, pid, LockType.EXCLUSIVE);
                newLocks.add(exclusiveLock);
                locksPerPage.put(pid, newLocks);
                return true;
            }
        }
        return false;
    }

    public synchronized boolean releaseLock(PageId pid) {
        return releaseLock(null, pid);
    }

    public synchronized boolean releaseLock(TransactionId tid, PageId pid) {
        // check tx status
        List<Lock> locks = locksPerPage.get(pid);
        if (locks == null || locks.isEmpty()) {
            locksPerPage.remove(pid);
            return false;
        }
        if (tid == null) {
            locksPerPage.remove(pid);
            return true;
        }
        boolean ret = locks.removeIf((lock) -> lock.getTid().equals(tid));
        if (locks.isEmpty()) {
            locksPerPage.remove(pid);
        }
        return ret;
    }

    public synchronized boolean hasLock(TransactionId tid, PageId pid) {
        List<Lock> locks = locksPerPage.get(pid);
        if (locks == null) {
            return false;
        }
        for (Lock lock : locks) {
            if (lock.getTid().equals(tid)) {
                return true;
            }
        }
        return false;
    }

    // get the owner transaction of this page
    public TransactionId getOwner(PageId pid) {
        List<Lock> locks = locksPerPage.get(pid);
        if(locks == null) {
            return null;
        }
        if(locks.size() == 1 && locks.get(0).getType() == LockType.EXCLUSIVE) {
            return locks.get(0).getTid();
        }
        return null;
    }

    public boolean detectDeadLock() {
        return false;
    }
}
