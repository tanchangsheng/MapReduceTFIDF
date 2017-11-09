package aa.vectorisation;

import java.util.List;
import java.util.Map;

/**
 * @author kevinsteppe
 */
public interface Mapper<T, MK, MV>
{
    /**
     * Implement this to provide the mapping function.
     * It is STRONGLY recommended that the map have no side-effects (no use of non-local variables).
     * @param list the data shard for this mapper
     * @return a map of key-value pairs which will be combined from all mappers and given to the reducers
     *
     * Generics note:
     *
     * T stands for incoming data's type
     * MK stands for Mapper's Key type
     * MV stands for Mapper's Value type
     *
     * thus:
     * Mapper<T, MK, MV> emits a map of MK to MV, given a list of data type T.
     */
    public Map<MK, MV> map(List<T> list);
}
