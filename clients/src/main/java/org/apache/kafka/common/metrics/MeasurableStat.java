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
package org.apache.kafka.common.metrics;

/**
 * {@link MeasurableStat}是{@link Stat}和{@link Measurable}的结合体（比如，可以生产单个浮点值）
 * 接口用于绝大多数的简单数据分析，比如{@link org.apache.kafka.common.metrics.stats.Avg}、{@link org.apache.kafka.common.metrics.stats.Max}、
 * {@link org.apache.kafka.common.metrics.stats.CumulativeCount}等
 */
public interface MeasurableStat extends Stat, Measurable {

}
