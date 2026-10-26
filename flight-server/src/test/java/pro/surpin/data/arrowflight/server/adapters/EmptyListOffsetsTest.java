package pro.surpin.data.arrowflight.server.adapters;

import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Tests for {@link EmptyListOffsets}. */
@Tag("unit")
class EmptyListOffsetsTest {

    /** Verifies the Arrow behaviour the guard exists for: an empty list moves the shared buffer's index. */
    @Test
    void anEmptyListWritesTheSharedEmptyBuffer() {
        try (BufferAllocator allocator = new RootAllocator();
                ListVector list = ListVector.empty("list", allocator)) {
            ArrowBuf shared = allocator.getEmpty();
            assertSame(shared, list.getOffsetBuffer());
            try {
                list.getFieldBuffers();
                assertEquals(4, shared.writerIndex());
            } finally {
                shared.writerIndex(0);
            }
        }
    }

    /** Verifies empty list and map children of a SqlInfo answer get own buffers, and unloading leaves the shared one alone. */
    @Test
    void sqlInfoRootNoLongerTouchesTheSharedEmptyBuffer() {
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_SQL_INFO_SCHEMA, allocator)) {
            ArrowBuf shared = allocator.getEmpty();
            DenseUnionVector values = (DenseUnionVector) root.getVector("value");
            values.setTypeId(0, (byte) 3);
            org.apache.arrow.vector.holders.NullableIntHolder holder =
                    new org.apache.arrow.vector.holders.NullableIntHolder();
            holder.isSet = 1;
            holder.value = 0;
            values.setSafe(0, holder);
            ((org.apache.arrow.vector.UInt4Vector) root.getVector("info_name")).setSafe(0, 8);
            root.setRowCount(1);

            EmptyListOffsets.own(root);

            ListVector stringList = values.getList((byte) 4);
            ListVector map = (ListVector) values.getChildrenFromFields().get(5);
            assertNotSame(shared, stringList.getOffsetBuffer());
            assertNotSame(shared, map.getOffsetBuffer());
            assertEquals(0, stringList.getValueCount());
            try (ArrowRecordBatch batch = new VectorUnloader(root).getRecordBatch()) {
                assertEquals(0, shared.writerIndex());
                assertEquals(1, batch.getLength());
            }
            assertEquals(0, ((IntVector) values.getChildrenFromFields().get(3)).get(0));
        }
    }

    /** Verifies a list with values keeps its data and buffer. */
    @Test
    void aListWithValuesIsLeftAsIs() {
        try (BufferAllocator allocator = new RootAllocator();
                ListVector list = ListVector.empty("list", allocator)) {
            UnionListWriter writer = list.getWriter();
            writer.setPosition(0);
            writer.startList();
            writer.writeInt(7);
            writer.endList();
            list.setValueCount(1);
            ArrowBuf offsets = list.getOffsetBuffer();
            EmptyListOffsets.own(VectorSchemaRoot.of(list));

            assertSame(offsets, list.getOffsetBuffer());
            assertEquals(List.of(7), list.getObject(0));
        }
    }

    /** Verifies the guard fixes the root before each batch and passes every call on. */
    @Test
    void guardFixesEachBatchBeforePassingItOn() {
        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_SQL_INFO_SCHEMA, allocator)) {
            FlightProducer.ServerStreamListener delegate = mock(FlightProducer.ServerStreamListener.class);
            FlightProducer.ServerStreamListener guard = EmptyListOffsets.guard(delegate);
            DenseUnionVector values = (DenseUnionVector) root.getVector("value");

            guard.start(root);
            assertNotSame(allocator.getEmpty(), values.getList((byte) 4).getOffsetBuffer());
            values.getList((byte) 4).clear();
            assertSame(allocator.getEmpty(), values.getList((byte) 4).getOffsetBuffer());
            guard.putNext();
            assertNotSame(allocator.getEmpty(), values.getList((byte) 4).getOffsetBuffer());
            guard.completed();
            Runnable handler = () -> { };
            guard.setOnReadyHandler(handler);
            guard.setOnCancelHandler(handler);
            guard.setUseZeroCopy(true);

            var order = inOrder(delegate);
            order.verify(delegate).start(any(VectorSchemaRoot.class), any(), any(IpcOption.class));
            order.verify(delegate).putNext();
            order.verify(delegate).completed();
            verify(delegate).setOnReadyHandler(handler);
            verify(delegate).setOnCancelHandler(handler);
            verify(delegate).setUseZeroCopy(true);
        }
    }
}
