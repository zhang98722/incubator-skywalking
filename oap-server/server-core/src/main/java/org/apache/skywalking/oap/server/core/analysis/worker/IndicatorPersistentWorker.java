/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.oap.server.core.analysis.worker;

import java.util.*;
import org.apache.skywalking.apm.commons.datacarrier.DataCarrier;
import org.apache.skywalking.apm.commons.datacarrier.consumer.*;
import org.apache.skywalking.oap.server.core.UnexpectedException;
import org.apache.skywalking.oap.server.core.analysis.data.*;
import org.apache.skywalking.oap.server.core.analysis.indicator.Indicator;
import org.apache.skywalking.oap.server.core.storage.IIndicatorDAO;
import org.apache.skywalking.oap.server.core.worker.AbstractWorker;
import org.apache.skywalking.oap.server.library.module.ModuleManager;
import org.slf4j.*;

import static java.util.Objects.nonNull;

/**
 * @author peng-yongsheng
 */
public class IndicatorPersistentWorker extends PersistenceWorker<Indicator, MergeDataCache<Indicator>> {

    private static final Logger logger = LoggerFactory.getLogger(IndicatorPersistentWorker.class);

    private final String modelName;
    private final MergeDataCache<Indicator> mergeDataCache;
    private final IIndicatorDAO indicatorDAO;
    private final AbstractWorker<Indicator> nextAlarmWorker;
    private final AbstractWorker<Indicator> nextExportWorker;
    private final DataCarrier<Indicator> dataCarrier;

    IndicatorPersistentWorker(int workerId, String modelName, int batchSize, ModuleManager moduleManager,
        IIndicatorDAO indicatorDAO, AbstractWorker<Indicator> nextAlarmWorker,
        AbstractWorker<Indicator> nextExportWorker) {
        super(moduleManager, workerId, batchSize);
        this.modelName = modelName;
        this.mergeDataCache = new MergeDataCache<>();
        this.indicatorDAO = indicatorDAO;
        this.nextAlarmWorker = nextAlarmWorker;
        this.nextExportWorker = nextExportWorker;

        String name = "INDICATOR_L2_AGGREGATION";
        int size = BulkConsumePool.Creator.recommendMaxSize() / 8;
        if (size == 0) {
            size = 1;
        }
        BulkConsumePool.Creator creator = new BulkConsumePool.Creator(name, size, 20);
        try {
            ConsumerPoolFactory.INSTANCE.createIfAbsent(name, creator);
        } catch (Exception e) {
            throw new UnexpectedException(e.getMessage(), e);
        }

        this.dataCarrier = new DataCarrier<>("IndicatorPersistentWorker." + modelName, name, 1, 2000);
        this.dataCarrier.consume(ConsumerPoolFactory.INSTANCE.get(name), new PersistentConsumer(this));
    }

    @Override void onWork(Indicator indicator) {
        super.onWork(indicator);
    }

    @Override public void in(Indicator indicator) {
        indicator.setEndOfBatchContext(new EndOfBatchContext(false));
        dataCarrier.produce(indicator);
    }

    @Override public MergeDataCache<Indicator> getCache() {
        return mergeDataCache;
    }

    public boolean flushAndSwitch() {
        boolean isSwitch;
        try {
            if (isSwitch = getCache().trySwitchPointer()) {
                getCache().switchPointer();
            }
        } finally {
            getCache().trySwitchPointerFinally();
        }
        return isSwitch;
    }

    @Override public List<Object> prepareBatch(MergeDataCache<Indicator> cache) {
        List<Object> batchCollection = new LinkedList<>();
        cache.getLast().collection().forEach(data -> {
            Indicator dbData = null;
            try {
                dbData = indicatorDAO.get(modelName, data);
            } catch (Throwable t) {
                logger.error(t.getMessage(), t);
            }
            try {
                if (nonNull(dbData)) {
                    data.combine(dbData);
                    data.calculate();

                    batchCollection.add(indicatorDAO.prepareBatchUpdate(modelName, data));
                } else {
                    batchCollection.add(indicatorDAO.prepareBatchInsert(modelName, data));
                }

                if (Objects.nonNull(nextAlarmWorker)) {
                    nextAlarmWorker.in(data);
                }
                if (Objects.nonNull(nextExportWorker)) {
                    nextExportWorker.in(data);
                }
            } catch (Throwable t) {
                logger.error(t.getMessage(), t);
            }
        });

        return batchCollection;
    }

    @Override public void cacheData(Indicator input) {
        mergeDataCache.writing();
        if (mergeDataCache.containsKey(input)) {
            Indicator indicator = mergeDataCache.get(input);
            indicator.combine(input);
            indicator.calculate();
        } else {
            input.calculate();
            mergeDataCache.put(input);
        }

        mergeDataCache.finishWriting();
    }

    private class PersistentConsumer implements IConsumer<Indicator> {

        private final IndicatorPersistentWorker persistent;

        private PersistentConsumer(IndicatorPersistentWorker persistent) {
            this.persistent = persistent;
        }

        @Override public void init() {

        }

        @Override public void consume(List<Indicator> data) {
            Iterator<Indicator> inputIterator = data.iterator();

            int i = 0;
            while (inputIterator.hasNext()) {
                Indicator indicator = inputIterator.next();
                i++;
                if (i == data.size()) {
                    indicator.getEndOfBatchContext().setEndOfBatch(true);
                }
                persistent.onWork(indicator);
            }
        }

        @Override public void onError(List<Indicator> data, Throwable t) {
            logger.error(t.getMessage(), t);
        }

        @Override public void onExit() {
        }
    }
}
