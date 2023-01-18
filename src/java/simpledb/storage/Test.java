package simpledb.storage;

/**
 * @author kevin.zeng
 * @description
 * @create 2023-01-18
 */
public class Test {
    private PageId _pid;
    private int _tupleNo;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Test test = (Test) o;

        if (_tupleNo != test._tupleNo) return false;
        return _pid.equals(test._pid);
    }

    @Override
    public int hashCode() {
        int result = _pid.hashCode();
        result = 31 * result + _tupleNo;
        return result;
    }
}
