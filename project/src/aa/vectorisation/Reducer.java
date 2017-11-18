package aa.vectorisation;

import java.util.List;
import java.util.Map;

public interface Reducer<MK, MV, RK, RV>
{
    /**
     * Implement this to provide the reducer function.
     * It is STRONGLY recommended that the reducer have no side-effects (no use of non-local variables).
     * @param key the key given to this reducer to work on; only one key is given, all values for that key are given
     * @param data a list of all values for this key
     * @return a map of key-value pairs which will be placed in the final output.  Normally this will be a one entry
     *           map, with the provided key and the results of the reduce
     *
     * Generics note:
     *
     * (incoming data)
     * MK stands for Mapper's Key type
     * MV stands for Mapper's Value type
     *
     * (outgoing data)
     * RK stands for Reducer's Key type
     * RV stands for Reducer's Value type
     *
     * thus:
     * Reducer<MK, MV, RK, RV> receives key of type MK, a List<MV> and emits a map of RK to RV.
     */
    public Map<RK, RV> reduce(MK key, List<MV> data);
}
