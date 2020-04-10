package com.alonzo.hive.udaf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.HashMap;
import java.util.Map;


@Description(name = "mostOccrItem", value = "_FUNC_(x) - Returns a  object that occures most. "
        + "CAUTION will easily cause Out Of Memmory Exception on large data sets")
public class MostOccuItem extends AbstractGenericUDAFResolver {

    public MostOccuItem() {
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }
        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                            + parameters[0].getTypeName()
                            + " was passed as parameter 1.");
        }
        return new GenericUDAFMkListEvaluator();
    }

    public static class GenericUDAFMkListEvaluator extends GenericUDAFEvaluator {
        //	private PrimitiveObjectInspector inputOI;
        private StandardMapObjectInspector mapOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            if (m == Mode.PARTIAL1) {
                //inputOI = (PrimitiveObjectInspector) parameters[0];
                return ObjectInspectorFactory
                        .getStandardMapObjectInspector((PrimitiveObjectInspector) ObjectInspectorUtils
                                .getStandardObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector), PrimitiveObjectInspectorFactory.javaIntObjectInspector);
            } else if (m == Mode.PARTIAL2) {
                mapOI = (StandardMapObjectInspector) parameters[0];
                return ObjectInspectorFactory
                        .getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector, PrimitiveObjectInspectorFactory.javaIntObjectInspector);
            } else if (m == Mode.FINAL) {
                mapOI = (StandardMapObjectInspector) parameters[0];
                return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            } else if (m == Mode.COMPLETE) {
                return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            } else {
                throw new RuntimeException("no such mode Exception");
            }
        }

        static class MkArrayAggregationBuffer implements AggregationBuffer {
            Map<String, Integer> container;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((MkArrayAggregationBuffer) agg).container = new HashMap<String, Integer>();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MkArrayAggregationBuffer ret = new MkArrayAggregationBuffer();
            reset(ret);
            return ret;
        }

        // Mapside
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            assert (parameters.length == 1);
            Object p = parameters[0];
            if (p != null) {
                MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
                putIntoMap(p.toString(), myagg, 1);
            }
        }

        // Mapside
        @Override
        public Object terminatePartial(AggregationBuffer agg)
                throws HiveException {
            MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
            Map<String, Integer> ret = new HashMap<String, Integer>(
                    myagg.container);

            return ret;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;

            Map partialResult = mapOI.getMap(partial);

            for (Object key : partialResult.keySet()) {

                putIntoMap(key.toString(), myagg, Integer.valueOf(partialResult.get(key).toString()));
            }
        }

        @Override
        public String terminate(AggregationBuffer agg) throws HiveException {
            Map<Object, Integer> map = new HashMap<Object, Integer>();
            MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
            int num = Integer.MIN_VALUE;
            String key = null;
            for (Map.Entry<String, Integer> entry : myagg.container.entrySet()) {
                if (num < entry.getValue()) {
                    num = entry.getValue();
                    key = entry.getKey();
                }
            }
            return key;
        }

        private void putIntoMap(String p, MkArrayAggregationBuffer myagg, int num) {
            //	Object pCopy = ObjectInspectorUtils.copyToStandardObject(p,this.inputOI);
            Integer i = myagg.container.get(p);
            if (i == null) {
                i = num;
            } else {
                i = i + num;
            }
            myagg.container.put(p, i);
        }
    }
}
