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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.LogCaptureAppender;
import org.easymock.EasyMockRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.rocksdb.*;
import org.rocksdb.AbstractCompactionFilter.Context;
import org.rocksdb.util.BytewiseComparator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

import static org.easymock.EasyMock.*;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.Assert.fail;

/**
 * The purpose of this test is, to catch interface changes if we upgrade {@link RocksDB}.
 * Using reflections, we make sure the {@link RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter} maps all
 * methods from {@link DBOptions} and {@link ColumnFamilyOptions} to/from {@link Options} correctly.
 */
@RunWith(EasyMockRunner.class)
public class RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapterTest {

    private final List<String> walRelatedMethods = new LinkedList<String>() {
        {
            add("setManualWalFlush");
            add("setMaxTotalWalSize");
            add("setWalBytesPerSync");
            add("setWalDir");
            add("setWalFilter");
            add("setWalRecoveryMode");
            add("setWalSizeLimitMB");
            add("setWalTtlSeconds");
        }
    };

    private final List<String> ignoreMethods = new LinkedList<String>() {
        {
            add("isOwningHandle");
            add("getNativeHandle");
            add("dispose");
            add("wait");
            add("equals");
            add("getClass");
            add("hashCode");
            add("notify");
            add("notifyAll");
            add("toString");
            add("getOptionStringFromProps");
            addAll(walRelatedMethods);
        }
    };

    @Test
    public void shouldOverwriteAllOptionsMethods() throws Exception {
        for (final Method method : Options.class.getMethods()) {
            if (!ignoreMethods.contains(method.getName())) {
                RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter.class
                    .getDeclaredMethod(method.getName(), method.getParameterTypes());
            }
        }
    }

    @Test
    public void shouldForwardAllDbOptionsCalls() throws Exception {
        for (final Method method : Options.class.getMethods()) {
            if (!ignoreMethods.contains(method.getName())) {
                try {
                    DBOptions.class.getMethod(method.getName(), method.getParameterTypes());
                    verifyDBOptionsMethodCall(method);
                } catch (final NoSuchMethodException expectedAndSwallow) { }
            }
        }
    }

    private void verifyDBOptionsMethodCall(final Method method) throws Exception {
        final DBOptions mockedDbOptions = mock(DBOptions.class);
        final RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter optionsFacadeDbOptions = new RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter(mockedDbOptions, new ColumnFamilyOptions());

        final Object[] parameters = getDBOptionsParameters(method.getParameterTypes());

        try {
            reset(mockedDbOptions);
            replay(mockedDbOptions);
            method.invoke(optionsFacadeDbOptions, parameters);
            verify();
            fail("Should have called DBOptions." + method.getName() + "()");
        } catch (final InvocationTargetException undeclaredMockMethodCall) {
            assertThat(undeclaredMockMethodCall.getCause(), instanceOf(AssertionError.class));
            assertThat(undeclaredMockMethodCall.getCause().getMessage().trim(), matchesPattern("Unexpected method call DBOptions\\." + method.getName() + "((.*\n*)*):"));
        } finally {
            resetToNice(mockedDbOptions);
            optionsFacadeDbOptions.close();
        }
    }

    private Object[] getDBOptionsParameters(final Class<?>[] parameterTypes) throws Exception {
        final Object[] parameters = new Object[parameterTypes.length];

        for (int i = 0; i < parameterTypes.length; ++i) {
            switch (parameterTypes[i].getName()) {
                case "boolean":
                    parameters[i] = true;
                    break;
                case "int":
                    parameters[i] = 0;
                    break;
                case "long":
                    parameters[i] = 0L;
                    break;
                case "java.util.Collection":
                    parameters[i] = new ArrayList<>();
                    break;
                case "org.rocksdb.AccessHint":
                    parameters[i] = AccessHint.NONE;
                    break;
                case "org.rocksdb.Cache":
                    parameters[i] = new LRUCache(1L);
                    break;
                case "org.rocksdb.Env":
                    parameters[i] = Env.getDefault();
                    break;
                case "org.rocksdb.InfoLogLevel":
                    parameters[i] = InfoLogLevel.FATAL_LEVEL;
                    break;
                case "org.rocksdb.Logger":
                    parameters[i] = new Logger(new Options()) {
                        @Override
                        protected void log(final InfoLogLevel infoLogLevel, final String logMsg) {}
                    };
                    break;
                case "org.rocksdb.RateLimiter":
                    parameters[i] = new RateLimiter(1L);
                    break;
                case "org.rocksdb.SstFileManager":
                    parameters[i] = new SstFileManager(Env.getDefault());
                    break;
                case "org.rocksdb.WALRecoveryMode":
                    parameters[i] = WALRecoveryMode.AbsoluteConsistency;
                    break;
                case "org.rocksdb.WriteBufferManager":
                    parameters[i] = new WriteBufferManager(1L, new LRUCache(1L));
                    break;
                case "org.rocksdb.AbstractWalFilter":
                    class TestWalFilter extends AbstractWalFilter {
                        @Override
                        public void columnFamilyLogNumberMap(final Map<Integer, Long> cfLognumber, final Map<String, Integer> cfNameId) {
                        }

                        @Override
                        public LogRecordFoundResult logRecordFound(final long logNumber, final String logFileName, final WriteBatch batch, final WriteBatch newBatch) {
                            return new LogRecordFoundResult(WalProcessingOption.CONTINUE_PROCESSING, false);
                        }

                        @Override
                        public String name() {
                            return "TestWalFilter";
                        }
                    }
                    parameters[i] = new TestWalFilter();
                    break;
                default:
                    parameters[i] = parameterTypes[i].getConstructor().newInstance();
            }
        }

        return parameters;
    }

    @Test
    public void shouldForwardAllColumnFamilyCalls() throws Exception {
        for (final Method method : Options.class.getMethods()) {
            if (!ignoreMethods.contains(method.getName())) {
                try {
                    ColumnFamilyOptions.class.getMethod(method.getName(), method.getParameterTypes());
                    verifyColumnFamilyOptionsMethodCall(method);
                } catch (final NoSuchMethodException expectedAndSwallow) { }
            }
        }
    }

    private void verifyColumnFamilyOptionsMethodCall(final Method method) throws Exception {
        final ColumnFamilyOptions mockedColumnFamilyOptions = mock(ColumnFamilyOptions.class);
        final RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter optionsFacadeColumnFamilyOptions = new RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter(new DBOptions(), mockedColumnFamilyOptions);

        final Object[] parameters = getColumnFamilyOptionsParameters(method.getParameterTypes());

        try {
            reset(mockedColumnFamilyOptions);
            replay(mockedColumnFamilyOptions);
            method.invoke(optionsFacadeColumnFamilyOptions, parameters);
            verify();
            fail("Should have called ColumnFamilyOptions." + method.getName() + "()");
        } catch (final InvocationTargetException undeclaredMockMethodCall) {
            assertThat(undeclaredMockMethodCall.getCause(), instanceOf(AssertionError.class));
            assertThat(undeclaredMockMethodCall.getCause().getMessage().trim(), matchesPattern("Unexpected method call ColumnFamilyOptions\\." + method.getName() + "(.*)"));
        } finally {
            resetToNice(mockedColumnFamilyOptions);
            optionsFacadeColumnFamilyOptions.close();
        }
    }

    private Object[] getColumnFamilyOptionsParameters(final Class<?>[] parameterTypes) throws Exception {
        final Object[] parameters = new Object[parameterTypes.length];

        for (int i = 0; i < parameterTypes.length; ++i) {
            switch (parameterTypes[i].getName()) {
                case "boolean":
                    parameters[i] = true;
                    break;
                case "double":
                    parameters[i] = 0.0d;
                    break;
                case "int":
                    parameters[i] = 0;
                    break;
                case "long":
                    parameters[i] = 0L;
                    break;
                case "[I":
                    parameters[i] = new int[0];
                    break;
                case "java.util.List":
                    parameters[i] = new ArrayList<>();
                    break;
                case "org.rocksdb.AbstractCompactionFilter":
                    parameters[i] = new RemoveEmptyValueCompactionFilter();
                    break;
                case "org.rocksdb.AbstractCompactionFilterFactory":
                    parameters[i] = new AbstractCompactionFilterFactory<AbstractCompactionFilter<?>>() {

                        @Override
                        public AbstractCompactionFilter<?> createCompactionFilter(final Context context) {
                            return null;
                        }

                        @Override
                        public String name() {
                            return "AbstractCompactionFilterFactory";
                        }
                    };
                    break;
                case "org.rocksdb.AbstractComparator":
                    parameters[i] = new BytewiseComparator(new ComparatorOptions());
                    break;
                case "org.rocksdb.BuiltinComparator":
                    parameters[i] = BuiltinComparator.BYTEWISE_COMPARATOR;
                    break;
                case "org.rocksdb.CompactionPriority":
                    parameters[i] = CompactionPriority.ByCompensatedSize;
                    break;
                case "org.rocksdb.CompactionStyle":
                    parameters[i] = CompactionStyle.UNIVERSAL;
                    break;
                case "org.rocksdb.CompressionType":
                    parameters[i] = CompressionType.NO_COMPRESSION;
                    break;
                case "org.rocksdb.MemTableConfig":
                    parameters[i] = new VectorMemTableConfig();
                    break;
                case "org.rocksdb.MergeOperator":
                    parameters[i] = new StringAppendOperator();
                    break;
                case "org.rocksdb.TableFormatConfig":
                    parameters[i] = new PlainTableConfig();
                    break;
                default:
                    parameters[i] = parameterTypes[i].getConstructor().newInstance();
            }
        }

        return parameters;
    }

    @Test
    public void shouldLogWarningWhenSettingWalOptions() throws Exception {

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter.class)) {

            try (RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter adapter = new RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter(new DBOptions(), new ColumnFamilyOptions())) {
                for (final Method method : RocksDBGenericOptionsToDbOptionsColumnFamilyOptionsAdapter.class.getDeclaredMethods()) {
                    if (walRelatedMethods.contains(method.getName())) {
                        method.invoke(adapter, getDBOptionsParameters(method.getParameterTypes()));
                    }
                }

                final List<String> walOptions = Arrays.asList("walDir", "walFilter", "walRecoveryMode", "walBytesPerSync", "walSizeLimitMB", "manualWalFlush", "maxTotalWalSize", "walTtlSeconds");

                final Set<String> logMessages = appender.getEvents().stream().filter(e -> e.getLevel().equals("WARN")).map(LogCaptureAppender.Event::getMessage).collect(Collectors.toSet());

                walOptions.forEach(option -> assertThat(logMessages, hasItem(String.format("WAL is explicitly disabled by Streams in RocksDB. Setting option '%s' will be ignored", option))));
            }
        }
    }
}
