/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log.remote;

import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.RemoteLogReadResult;
import org.apache.kafka.storage.internals.log.RemoteStorageFetchInfo;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class RemoteLogReader implements Callable<Void> {
    private final Logger logger;
    private final RemoteStorageFetchInfo fetchInfo;
    private final RemoteLogManager rlm;
    private final Consumer<RemoteLogReadResult> callback;

    public RemoteLogReader(RemoteStorageFetchInfo fetchInfo, RemoteLogManager rlm, Consumer<RemoteLogReadResult> callback) {
        this.fetchInfo = fetchInfo;
        this.rlm = rlm;
        this.callback = callback;
        logger = new LogContext() {
            @Override
            public String logPrefix() {
                return "[" + Thread.currentThread().getName() + "]";
            }
        }.logger(RemoteLogReader.class);
    }

    @Override
    public Void call() {
        RemoteLogReadResult result;
        try {
            logger.debug("Reading records from remote storage for topic partition {}", fetchInfo.topicPartition);

            FetchDataInfo fetchDataInfo = rlm.read(fetchInfo);
            result = new RemoteLogReadResult(Optional.of(fetchDataInfo), Optional.empty());
        } catch (OffsetOutOfRangeException e) {
            result = new RemoteLogReadResult(Optional.empty(), Optional.of(e));
        } catch (Exception e) {
            logger.error("Error occurred while reading the remote data for {}", fetchInfo.topicPartition, e);
            result = new RemoteLogReadResult(Optional.empty(), Optional.of(e));
        }

        logger.debug("Finished reading records from remote storage for topic partition {}", fetchInfo.topicPartition);
        callback.accept(result);

        return null;
    }
}
