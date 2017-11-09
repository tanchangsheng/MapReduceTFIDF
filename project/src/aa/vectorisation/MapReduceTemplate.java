package aa.vectorisation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by kevinsteppe on 12/10/16.
 */
public class MapReduceTemplate
        implements  Mapper<DataType, MapKeyType, MapValueType>,
                    Reducer<MapKeyType, MapValueType, ReducerKeyType, ReducerValueType>
{

    @Override
    public Map<MapKeyType, MapValueType> map(List<DataType> list)
    {
        Map<MapKeyType, MapValueType> map = new HashMap<>();

        for (DataType o : list)
        {
            // Determine Key
            String key;

            // animal is column

            // Determine Value
            MapValueType newValue = ... ?

            // Put value into map.

            MapValueType currentValue = map.get(key);

            if (currentValue == null)
            {
                map.put(key, newValue);
            }
            else
            {
                //combine currentValue with newValue
                newValue += currentValue; //or whatever
                map.put(key, newValue);
            }

        }

        return map;
    }

    @Override
    public Map<ReducerKeyType, ReducerValueType> reduce(MapKeyType key, List<MapValueType> data)
    {
        HashMap<ReducerKeyType, ReducerValueType> map = new HashMap<>();

        for (MapValueType o : data)
        {
            //get data

            //do something with value
            //output += value; //<-- sum?
        }

        map.put(key, output);

        return map;
    }

    public static void main(String[] args)
    {
        //get the data
        List<DataType> data = .....

        //Create instance of the Mapper
        Mapper<DataType, MapKeyType, MapValueType> mapperCode = ....

        //Create instance of the Reducer
        Reducer<MapKeyType, MapValueType, ReducerKeyType, ReducerValueType> reducerCode = ....

        //How many shards?
        int numberOfShards = ...

        //lots of output?
        boolean verbose = ....

        Map<ReducerKeyType, ReducerValueType> resultMap;
        try {
            resultMap = MapReduce.mapReduce(mapperCode,
                                            reducerCode,
                                            data,
                                            numberOfShards,
                                            verbose);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
        }

        //Do something with resultMap
        // Example to output:
//        System.out.println("Output from reducers: ");
//        for (ReducerKeyType key : results.keySet())
//        {
//            System.out.println(key + " " + results.get(key).toString());
//        }


    }
}
