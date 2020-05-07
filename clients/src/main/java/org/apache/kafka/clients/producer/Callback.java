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
package org.apache.kafka.clients.producer;

/**
 * Callback接口用于在完成请求时，开发者可以继续实现的代码
 * Callback任务会在很快地在后台线程中完成
 */
public interface Callback {

    /**
     * callback()方法可以使开发者实现提供对请求完成的异步处理
	 * 如果record发送到了Kafka服务端，并且服务端已知晓，那么callback()方法就会被调用
	 * 如果在callback()中，exception不为null，元数据将会特殊的-1来标识所有字段，除了topic-partition对象，因为topic-partition是可用的
     * @param metadata 发送record的元数据（比如partition序号和偏移量）
	 *                 如果在发送过程中出现了错误，则除了特殊值之外的所有制都会为-1
     * @param exception record在发送过程中出现的异常，若无异常，则为null
     *                 	不可恢复的异常，消息不会被重新发送：
     *                  InvalidTopicException
     *                  OffsetMetadataTooLargeException
     *                  RecordBatchTooLargeException
     *                  RecordTooLargeException
     *                  UnknownServerException
     *					可恢复的异常，可能会被重新发送：
     *                  CorruptRecordException
     *                  InvalidMetadataException
     *                  NotEnoughReplicasAfterAppendException
     *                  NotEnoughReplicasException
     *                  OffsetOutOfRangeException
     *                  TimeoutException
     *                  UnknownTopicOrPartitionException
     */
    void onCompletion(RecordMetadata metadata, Exception exception);
}
