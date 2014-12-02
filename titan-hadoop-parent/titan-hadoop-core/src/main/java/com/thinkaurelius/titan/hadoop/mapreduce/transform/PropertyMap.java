package com.thinkaurelius.titan.hadoop.mapreduce.transform;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.StandardFaunusEdge;
import com.thinkaurelius.titan.hadoop.Tokens;
import com.thinkaurelius.titan.hadoop.mapreduce.util.ElementPicker;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;
import com.thinkaurelius.titan.hadoop.mapreduce.util.SafeMapperOutputs;
import com.thinkaurelius.titan.hadoop.mapreduce.util.WritableHandler;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyMap {

    public static final String CLASS = Tokens.makeNamespace(PropertyMap.class) + ".class";
    public static final String KEY = Tokens.makeNamespace(PropertyMap.class) + ".key";
    public static final String TYPE = Tokens.makeNamespace(PropertyMap.class) + ".type";

    public enum Counters {
        VERTICES_PROCESSED,
        OUT_EDGES_PROCESSED
    }

    public static Configuration createConfiguration(final Class<? extends Element> klass, final String key, final Class<? extends WritableComparable> type) {
        final Configuration configuration = new EmptyConfiguration();
        configuration.setClass(CLASS, klass, Element.class);
        configuration.set(KEY, key);
        configuration.setClass(TYPE, type, WritableComparable.class);
        return configuration;
    }

    public static class Map extends Mapper<NullWritable, FaunusVertex, NullWritable, WritableComparable> {

        private String key;
        private boolean isVertex;
        private WritableHandler handler;
        private SafeMapperOutputs outputs;

        @Override
        public void setup(final Mapper.Context context) throws IOException, InterruptedException {
            final Configuration cfg = DEFAULT_COMPAT.getContextConfiguration(context);
            this.isVertex = cfg.getClass(CLASS, Element.class, Element.class).equals(Vertex.class);
            this.key = cfg.get(KEY);
            this.handler = new WritableHandler(cfg.getClass(TYPE, Text.class, WritableComparable.class));
            this.outputs = new SafeMapperOutputs(context);
        }

        @Override
        public void map(final NullWritable key, final FaunusVertex value, final Mapper<NullWritable, FaunusVertex, NullWritable, WritableComparable>.Context context) throws IOException, InterruptedException {

            this.outputs.write(Tokens.GRAPH, NullWritable.get(), value);

            if (this.isVertex) {
                if (value.hasPaths()) {
                    WritableComparable writable = this.handler.set(ElementPicker.getProperty(value, this.key));
                    for (int i = 0; i < value.pathCount(); i++) {
                        this.outputs.write(Tokens.SIDEEFFECT, NullWritable.get(), writable);
                    }
                    DEFAULT_COMPAT.incrementContextCounter(context, Counters.VERTICES_PROCESSED, 1L);
                }
            } else {
                long edgesProcessed = 0;
                for (final Edge e : value.getEdges(Direction.OUT)) {
                    final StandardFaunusEdge edge = (StandardFaunusEdge) e;
                    if (edge.hasPaths()) {
                        WritableComparable writable = this.handler.set(ElementPicker.getProperty(edge, this.key));
                        for (int i = 0; i < edge.pathCount(); i++) {
                            this.outputs.write(Tokens.SIDEEFFECT, NullWritable.get(), writable);
                        }
                        edgesProcessed++;
                    }
                }
                DEFAULT_COMPAT.incrementContextCounter(context, Counters.OUT_EDGES_PROCESSED, edgesProcessed);
            }
        }

        @Override
        public void cleanup(final Mapper<NullWritable, FaunusVertex, NullWritable, WritableComparable>.Context context) throws IOException, InterruptedException {
            this.outputs.close();
        }
    }
}
