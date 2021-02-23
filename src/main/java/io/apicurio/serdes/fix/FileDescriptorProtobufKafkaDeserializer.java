/*
 * Copyright 2021 Red Hat
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.apicurio.serdes.fix;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.header.Headers;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.TimestampProto;
import com.google.protobuf.WrappersProto;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import io.apicurio.registry.common.proto.Serde;
import io.apicurio.registry.utils.serde.ProtobufKafkaDeserializer;
import io.apicurio.registry.utils.serde.util.HeaderUtils;
import io.apicurio.registry.utils.serde.util.Utils;

/**
 * @author Fabian Martinez
 */
public class FileDescriptorProtobufKafkaDeserializer extends ProtobufKafkaDeserializer {

    private static final FileDescriptor TIMESTAMP_SCHEMA = TimestampProto.getDescriptor();
    private static final FileDescriptor WRAPPER_SCHEMA = WrappersProto.getDescriptor();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        super.configure(configs, isKey);
        if (Utils.isTrue(configs.get(USE_HEADERS))) {
            // noinspection unchecked
            headerUtils = new HeaderUtils((Map<String, Object>) configs, isKey);
        }
    }

    @Override
    protected DynamicMessage readData(byte[] schema, ByteBuffer buffer, int start, int length) {
        try {
            // Serde.Schema s = Serde.Schema.parseFrom(schema);
            Descriptors.FileDescriptor fileDescriptor = toFileDescriptor(schema);

            byte[] bytes = new byte[length];
            System.arraycopy(buffer.array(), start, bytes, 0, length);
            ByteArrayInputStream is = new ByteArrayInputStream(bytes);

            Serde.Ref ref = Serde.Ref.parseDelimitedFrom(is);

            Descriptors.Descriptor descriptor = fileDescriptor.findMessageTypeByName(ref.getName());
            return DynamicMessage.parseFrom(descriptor, is);
        } catch (IOException | Descriptors.DescriptorValidationException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public DynamicMessage readData(Headers headers, byte[] schema, ByteBuffer buffer, int start, int length) {
        return readData(schema, buffer, start, length);
    }

    private Descriptors.FileDescriptor toFileDescriptor(byte[] schema) throws DescriptorValidationException, IOException {
        DescriptorProtos.FileDescriptorSet set = DescriptorProtos.FileDescriptorSet.parseFrom(schema);

        List<FileDescriptor> dependencyFileDescriptorList = new ArrayList<>();
        dependencyFileDescriptorList.add(TIMESTAMP_SCHEMA);
        dependencyFileDescriptorList.add(WRAPPER_SCHEMA);
        for (int i = 0; i < set.getFileCount() - 1; i++) {
            dependencyFileDescriptorList.add(Descriptors.FileDescriptor.buildFrom(set.getFile(i), dependencyFileDescriptorList.toArray(new FileDescriptor[i])));
        }

        return Descriptors.FileDescriptor.buildFrom(set.getFile(set.getFileCount() - 1), dependencyFileDescriptorList.toArray(new FileDescriptor[0]));
    }

}
