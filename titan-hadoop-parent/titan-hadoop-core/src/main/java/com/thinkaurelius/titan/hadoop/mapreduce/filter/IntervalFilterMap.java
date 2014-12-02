package com.thinkaurelius.titan.hadoop.mapreduce.filter;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.StandardFaunusEdge;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.thinkaurelius.titan.hadoop.mapreduce.util.ElementChecker;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IntervalFilterMap {

    public static final String CLASS = Tokens.makeNamespace(IntervalFilterMap.class) + ".class";
    public static final String KEY = Tokens.makeNamespace(IntervalFilterMap.class) + ".key";
    public static final String START_VALUE = Tokens.makeNamespace(IntervalFilterMap.class) + ".startValue";
    public static final String END_VALUE = Tokens.makeNamespace(IntervalFilterMap.class) + ".endValue";
    public static final String VALUE_CLASS = Tokens.makeNamespace(IntervalFilterMap.class) + ".valueClass";

    public enum Counters {
        VERTICES_FILTERED,
        EDGES_FILTERED
    }

    public static Configuration createConfiguration(final Class<? extends Element> klass, final String key, final Object startValue, final Object endValue) {
        final Configuration configuration = new EmptyConfiguration();
        configuration.setClass(CLASS, klass, Element.class);
        configuration.set(KEY, key);
        if (startValue instanceof String) {
            configuration.set(VALUE_CLASS, String.class.getName());
            configuration.set(START_VALUE, (String) startValue);
            configuration.set(END_VALUE, (String) endValue);
        } else if (startValue instanceof Number) {
            configuration.set(VALUE_CLASS, Float.class.getName());
            configuration.setFloat(START_VALUE, ((Number) startValue).floatValue());
            configuration.setFloat(END_VALUE, ((Number) endValue).floatValue());
        } else if (startValue instanceof Boolean) {
            configuration.set(VALUE_CLASS, Boolean.class.getName());
            configuration.setBoolean(START_VALUE, (Boolean) startValue);
            configuration.setBoolean(END_VALUE, (Boolean) endValue);
        } else {
            throw new RuntimeException("Unknown value class: " + startValue.getClass().getName());
        }
        return configuration;
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex> {

        private boolean isVertex;
        private ElementChecker startChecker;
        private ElementChecker endChecker;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            Configuration cfg = DEFAULT_COMPAT.getContextConfiguration(context);
            this.isVertex = cfg.getClass(CLASS, Element.class, Element.class).equals(Vertex.class);
            final String key = cfg.get(KEY);
            final Class valueClass = cfg.getClass(VALUE_CLASS, String.class);
            final Object startValue;
            final Object endValue;
            if (valueClass.equals(String.class)) {
                startValue = cfg.get(START_VALUE);
                endValue = cfg.get(END_VALUE);
            } else if (Number.class.isAssignableFrom((valueClass))) {
                startValue = cfg.getFloat(START_VALUE, Float.MIN_VALUE);
                endValue = cfg.getFloat(END_VALUE, Float.MAX_VALUE);
            } else {
                throw new IOException("Class " + valueClass + " is an unsupported value class");
            }

            this.startChecker = new ElementChecker(key, Compare.GREATER_THAN_EQUAL, startValue);
            this.endChecker = new ElementChecker(key, Compare.LESS_THAN, endValue);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, FaunusVertex>.Context context) throws IOException, InterruptedException {
            if (this.isVertex) {
                if (value.hasPaths() && !(this.startChecker.isLegal(value) && this.endChecker.isLegal(value))) {
                    value.clearPaths();
                    DEFAULT_COMPAT.incrementContextCounter(context, Counters.VERTICES_FILTERED, 1L);
                }
            } else {
                long counter = 0;
                for (final Edge e : value.getEdges(Direction.BOTH)) {
                    final StandardFaunusEdge edge = (StandardFaunusEdge) e;
                    if (edge.hasPaths() && !(this.startChecker.isLegal(edge) && this.endChecker.isLegal(edge))) {
                        edge.clearPaths();
                        counter++;
                    }
                }
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.EDGES_FILTERED, counter);
            }
            context.write(NullWritable.get(), value);
        }
    }
}
