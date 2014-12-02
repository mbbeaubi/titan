package com.thinkaurelius.titan.hadoop.formats.util.input;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.hadoop.FaunusVertexQueryFilter;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class TitanHadoopSetupCommon implements TitanHadoopSetup {

    private static final StaticBuffer DEFAULT_COLUMN = StaticArrayBuffer.of(new byte[0]);
    private static final SliceQuery DEFAULT_SLICE_QUERY = new SliceQuery(DEFAULT_COLUMN, DEFAULT_COLUMN);

    @Override
    public SliceQuery inputSlice(final FaunusVertexQueryFilter inputFilter) {
        //For now, only return the full range because the current input format needs to read the hidden
        //vertex-state property to determine if the vertex is a ghost. If we filter, that relation would fall out as well.
        return DEFAULT_SLICE_QUERY;
    }

    @Override
    public void close() {
        //Do nothing
    }

    public static SliceQuery getDefaultSliceQuery() {
        return DEFAULT_SLICE_QUERY;
    }

}
