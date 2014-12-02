package com.thinkaurelius.titan.hadoop.mapreduce.transform;

import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.StandardFaunusEdge;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.thinkaurelius.titan.hadoop.mapreduce.util.ElementPicker;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;
import com.thinkaurelius.titan.hadoop.mapreduce.util.SafeMapperOutputs;
import com.thinkaurelius.titan.hadoop.mapreduce.util.SafeReducerOutputs;
import com.thinkaurelius.titan.hadoop.mapreduce.util.WritableComparators;
import com.thinkaurelius.titan.hadoop.mapreduce.util.WritableHandler;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.pipes.transform.TransformPipe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class OrderMapReduce {

    public static final String CLASS = Tokens.makeNamespace(OrderMapReduce.class) + ".class";
    public static final String KEY = Tokens.makeNamespace(OrderMapReduce.class) + ".key";
    public static final String TYPE = Tokens.makeNamespace(OrderMapReduce.class) + ".type";
    public static final String ELEMENT_KEY = Tokens.makeNamespace(OrderMapReduce.class) + ".elementKey";

    public enum Counters {
        VERTICES_PROCESSED,
        OUT_EDGES_PROCESSED
    }

    public static Configuration createConfiguration(final Class<? extends Element> klass,
                                                    final String key,
                                                    final Class<? extends WritableComparable> type,
                                                    final String elementKey) {
        final Configuration configuration = new EmptyConfiguration();
        configuration.setClass(OrderMapReduce.CLASS, klass, Element.class);
        configuration.set(OrderMapReduce.KEY, key);
        configuration.setClass(OrderMapReduce.TYPE, type, WritableComparable.class);
        configuration.set(OrderMapReduce.ELEMENT_KEY, elementKey);
        return configuration;
    }

    public static Class<? extends WritableComparator> createComparator(final TransformPipe.Order order, final Class<? extends WritableComparable> comparable) {
        Class<? extends WritableComparator> comparatorClass = null;
        if (comparable.equals(LongWritable.class))
            comparatorClass = order.equals(TransformPipe.Order.INCR) ? LongWritable.Comparator.class : LongWritable.DecreasingComparator.class;
        else if (comparable.equals(IntWritable.class))
            comparatorClass = order.equals(TransformPipe.Order.INCR) ? IntWritable.Comparator.class : WritableComparators.DecreasingIntComparator.class;
        else if (comparable.equals(FloatWritable.class))
            comparatorClass = order.equals(TransformPipe.Order.INCR) ? FloatWritable.Comparator.class : WritableComparators.DecreasingFloatComparator.class;
        else if (comparable.equals(DoubleWritable.class))
            comparatorClass = order.equals(TransformPipe.Order.INCR) ? DoubleWritable.Comparator.class : WritableComparators.DecreasingDoubleComparator.class;
        else if (comparable.equals(Text.class))
            comparatorClass = order.equals(TransformPipe.Order.INCR) ? Text.Comparator.class : WritableComparators.DecreasingTextComparator.class;
        return comparatorClass;
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, WritableComparable, Text> {

        private String key;
        private boolean isVertex;
        private WritableHandler handler;
        private String elementKey;
        private SafeMapperOutputs outputs;


        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            final Configuration cfg = DEFAULT_COMPAT.getContextConfiguration(context);
            this.isVertex = cfg.getClass(CLASS, Element.class, Element.class).equals(Vertex.class);
            this.key = cfg.get(KEY);
            this.handler = new WritableHandler(cfg.getClass(TYPE, Text.class, WritableComparable.class));
            this.elementKey = cfg.get(ELEMENT_KEY);
            this.outputs = new SafeMapperOutputs(context);
        }

        private Text text = new Text();
        private WritableComparable writable;

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, WritableComparable, Text>.Context context) throws IOException, InterruptedException {
            if (this.isVertex) {
                if (value.hasPaths()) {
                    this.text.set(ElementPicker.getPropertyAsString(value, this.elementKey));
                    final Object temp = ElementPicker.getProperty(value, this.key);
                    if (this.key.equals(Tokens._COUNT)) {
                        this.writable = this.handler.set(temp);
                        context.write(this.writable, this.text);
                    } else if (temp instanceof Number) {
                        this.writable = this.handler.set(multiplyPathCount((Number) temp, value.pathCount()));
                        context.write(this.writable, this.text);
                    } else {
                        this.writable = this.handler.set(temp);
                        for (int i = 0; i < value.pathCount(); i++) {
                            context.write(this.writable, this.text);
                        }
                    }
                    DEFAULT_COMPAT.incrementContextCounter(context, Counters.VERTICES_PROCESSED, 1L);
                }
            } else {
                long edgesProcessed = 0;
                for (final Edge e : value.getEdges(Direction.OUT)) {
                    final StandardFaunusEdge edge = (StandardFaunusEdge) e;
                    if (edge.hasPaths()) {
                        this.text.set(ElementPicker.getPropertyAsString(edge, this.elementKey));
                        final Object temp = ElementPicker.getProperty(edge, this.key);
                        if (this.key.equals(Tokens._COUNT)) {
                            this.writable = this.handler.set(temp);
                            context.write(this.writable, this.text);
                        } else if (temp instanceof Number) {
                            this.writable = this.handler.set(multiplyPathCount((Number) temp, edge.pathCount()));
                            context.write(this.writable, this.text);
                        } else {
                            this.writable = this.handler.set(temp);
                            for (int i = 0; i < edge.pathCount(); i++) {
                                context.write(this.writable, this.text);
                            }
                        }
                        edgesProcessed++;
                    }
                }
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.OUT_EDGES_PROCESSED, edgesProcessed);
            }

            this.outputs.write(Tokens.GRAPH, NullWritable.get(), value);
        }

        @Override
        public void cleanup(final Mapper<NullWritable, FaunusVertex, WritableComparable, Text>.Context context) throws IOException, InterruptedException {
            this.outputs.close();
        }
    }

    public static class Reduce extends Reducer<WritableComparable, Text, Text, WritableComparable> {

        private SafeReducerOutputs outputs;

        @Override
        public void setup(final Reducer<WritableComparable, Text, Text, WritableComparable>.Context context) throws IOException, InterruptedException {
            this.outputs = new SafeReducerOutputs(context);
        }

        @Override
        public void reduce(final WritableComparable key, final Iterable<Text> values, final Reducer<WritableComparable, Text, Text, WritableComparable>.Context context) throws IOException, InterruptedException {
            for (final Text value : values) {
                this.outputs.write(Tokens.SIDEEFFECT, value, key);
            }
        }

        @Override
        public void cleanup(final Reducer<WritableComparable, Text, Text, WritableComparable>.Context context) throws IOException, InterruptedException {
            this.outputs.close();
        }
    }

    private static Number multiplyPathCount(final Number value, final Long pathCount) {
        if (value instanceof Long)
            return (Long) value * pathCount;
        else if (value instanceof Integer)
            return (Integer) value * pathCount;
        else if (value instanceof Double)
            return (Double) value * pathCount;
        else if (value instanceof Float)
            return (Float) value * pathCount;
        else
            return value.doubleValue() * pathCount;

    }
}
