package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.index.BTreeLeafPage;
import simpledb.index.BTreePageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import sun.security.krb5.internal.PAData;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    /** backing store binary File */
    private File file;
    /** each table has one HeapFile, TupleDesc determines size of HeapPage*/
    private TupleDesc schema;
    /** unique id */
    private int tableid;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.schema = td;
        this.tableid = f.getAbsolutePath().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return tableid;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return schema;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if(!(pid instanceof HeapPageId)) {
            throw new IllegalArgumentException();
        }
        if(pid.getTableId() != tableid) {
            throw new IllegalArgumentException();
        }
        // calculate offset of file
        int pageNo = pid.getPageNumber();
        long byteOffset = pageNo * BufferPool.getPageSize();
        // if offset overflow
        if(byteOffset > file.length()) {
            throw new IllegalArgumentException();
//            return null;
        }
        // random access by offset
        RandomAccessFile ra = null;
        byte[] pageData = HeapPage.createEmptyPageData();
        try {
            ra = new RandomAccessFile(file,"r");
            ra.seek(byteOffset);
            ra.read(pageData);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                ra.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // construct HeapPage
        HeapPageId hpid = (HeapPageId)pid;
        HeapPage page = null;
        try {
            page = new HeapPage(hpid,pageData);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1

        // append it to File
        // calculate offset of file
        int pageNo = page.getId().getPageNumber();
        long byteOffset = pageNo * BufferPool.getPageSize();
        // if offset overflow
        if(byteOffset > file.length()) {
            throw new IllegalArgumentException();
        }
        // random access by offset, write page into File
        RandomAccessFile ra = null;
        try {
            ra = new RandomAccessFile(file,"rw");
            ra.seek(byteOffset);
            ra.write(page.getPageData());
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                ra.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        int pageSize = BufferPool.getPageSize();
        long totalSize = file.length();
        int num = (int) (totalSize / pageSize);
        if(totalSize != 0 && totalSize % pageSize != 0) {
            num++;
        }
        return num;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> affectedPages = new ArrayList<>();
        // find a page with an empty slot
        for(int i = 0;i < numPages(); i++) {
            PageId pid = new HeapPageId(tableid,i);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
            // has an empty slot, insert and marked dirty
            if(heapPage.getNumEmptySlots() > 0) {
                heapPage.insertTuple(t);
                heapPage.markDirty(true,tid);
                affectedPages.add(heapPage);
                break;
            }
        }
        // not found, create a new page and append it to the physical file
        if(affectedPages.size() == 0) {
            HeapPageId pid = new HeapPageId(tableid,numPages());
            writePage(new HeapPage(pid,HeapPage.createEmptyPageData()));
            // fetch new page by BufferPool
            HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
            newPage.insertTuple(t);
            newPage.markDirty(true,tid);
            affectedPages.add(newPage);
        }
        return affectedPages;
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> affectedPage = new ArrayList<>();
        // check Tuple in this HeapFile
        PageId pid = t.getRecordId().getPageId();
        int tableId = pid.getTableId();
        if(tableId != tableid) {
            throw new DbException("deleteTuple: tableid mismatch");
        }
        if(pid.getPageNumber() > numPages() - 1) {
            throw new DbException("deleteTuple: page not exist");
        }
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
        heapPage.deleteTuple(t);
        heapPage.markDirty(true,tid);
        affectedPage.add(heapPage);
        return affectedPage;
    }

    class HeapFileIterator extends AbstractDbFileIterator {
        Iterator<Tuple> it = null;
        HeapPage curPage = null;

        final TransactionId tid;
        final HeapFile hf;

        public HeapFileIterator(TransactionId tid, HeapFile hf) {
            this.tid = tid;
            this.hf = hf;
        }

        /**
         * get the first page of HeapFile and initialize Iterator<Tuple>
         * @throws DbException
         * @throws TransactionAbortedException
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            HeapPageId firstPageId = new HeapPageId(tableid,0);
            curPage = (HeapPage) Database.getBufferPool().getPage(tid,firstPageId, Permissions.READ_ONLY);
            it = curPage.iterator();
        }

        /**
         * 	Read the next tuple either from the current page if it has more tuples or
         * 	from the next page by following the right sibling pointer.
         * @return
         */
        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            // curPage has no more page to iterate, set null
            if (it != null && !it.hasNext())
                it = null;

            while (it == null && curPage != null) {
                // whether reach the end page of DbFile
                HeapPageId nextp = null;
                HeapPageId curp = curPage.pid;
                if(curp.getPageNumber() < numPages() - 1) {
                    nextp = new HeapPageId(tableid,curp.getPageNumber() + 1);
                }
                if(nextp == null) {
                    curPage = null;
                }
                else {
                    // fetch next page
                    curPage = (HeapPage) Database.getBufferPool().getPage(tid,
                            nextp, Permissions.READ_WRITE);
                    it = curPage.iterator();
                    // page has not tuple
                    if (!it.hasNext())
                        it = null;
                }
            }

            if (it == null)
                return null;
            return it.next();
        }

        /**
         * rewind this iterator back to the beginning of the tuples
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        /**
         * close the iterator
         */
        @Override
        public void close() {
            super.close();
            it = null;
            curPage = null;
        }


    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, this);
    }

}

