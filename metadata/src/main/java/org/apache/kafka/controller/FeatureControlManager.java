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

package org.apache.kafka.controller;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.ZkMigrationStateRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.FinalizedControllerFeatures;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.metadata.migration.ZkMigrationState;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.mutable.BoundedList;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineObject;
import org.slf4j.Logger;

import java.util.*;
import java.util.Map.Entry;
import java.util.function.Consumer;

import static org.apache.kafka.common.metadata.MetadataRecordType.FEATURE_LEVEL_RECORD;
import static org.apache.kafka.controller.QuorumController.MAX_RECORDS_PER_USER_OP;


public class FeatureControlManager {

    public static class Builder {
        private LogContext logContext = null;
        private SnapshotRegistry snapshotRegistry = null;
        private QuorumFeatures quorumFeatures = null;
        private MetadataVersion metadataVersion = MetadataVersion.latest();
        private MetadataVersion minimumBootstrapVersion = MetadataVersion.MINIMUM_BOOTSTRAP_VERSION;

        Builder setLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder setSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        Builder setQuorumFeatures(QuorumFeatures quorumFeatures) {
            this.quorumFeatures = quorumFeatures;
            return this;
        }

        Builder setMetadataVersion(MetadataVersion metadataVersion) {
            this.metadataVersion = metadataVersion;
            return this;
        }

        Builder setMinimumBootstrapVersion(MetadataVersion minimumBootstrapVersion) {
            this.minimumBootstrapVersion = minimumBootstrapVersion;
            return this;
        }

        public FeatureControlManager build() {
            if (logContext == null)
                logContext = new LogContext();
            if (snapshotRegistry == null)
                snapshotRegistry = new SnapshotRegistry(logContext);
            if (quorumFeatures == null) {
                quorumFeatures = new QuorumFeatures(0, new ApiVersions(), QuorumFeatures.defaultFeatureMap(), Collections.emptyList());
            }
            return new FeatureControlManager(logContext, quorumFeatures, snapshotRegistry, metadataVersion, minimumBootstrapVersion);
        }
    }

    private final Logger log;

    /**
     * An immutable map containing the features supported by this controller's software.
     */
    private final QuorumFeatures quorumFeatures;

    /**
     * Maps feature names to finalized version ranges.
     */
    private final TimelineHashMap<String, Short> finalizedVersions;

    /**
     * The current metadata version
     */
    private final TimelineObject<MetadataVersion> metadataVersion;

    /**
     * The current ZK migration state
     */
    private final TimelineObject<ZkMigrationState> migrationControlState;

    /**
     * The minimum bootstrap version that we can't downgrade before.
     */
    private final MetadataVersion minimumBootstrapVersion;

    private FeatureControlManager(LogContext logContext, QuorumFeatures quorumFeatures, SnapshotRegistry snapshotRegistry, MetadataVersion metadataVersion, MetadataVersion minimumBootstrapVersion) {
        this.log = logContext.logger(FeatureControlManager.class);
        this.quorumFeatures = quorumFeatures;
        this.finalizedVersions = new TimelineHashMap<>(snapshotRegistry, 0);
        this.metadataVersion = new TimelineObject<>(snapshotRegistry, metadataVersion);
        this.minimumBootstrapVersion = minimumBootstrapVersion;
        this.migrationControlState = new TimelineObject<>(snapshotRegistry, ZkMigrationState.NONE);
    }

    ControllerResult<Map<String, ApiError>> updateFeatures(Map<String, Short> updates, Map<String, FeatureUpdate.UpgradeType> upgradeTypes, Map<Integer, Map<String, VersionRange>> brokerFeatures, boolean validateOnly) {
        TreeMap<String, ApiError> results = new TreeMap<>();
        List<ApiMessageAndVersion> records = BoundedList.newArrayBacked(MAX_RECORDS_PER_USER_OP);
        for (Entry<String, Short> entry : updates.entrySet()) {
            results.put(entry.getKey(), updateFeature(entry.getKey(), entry.getValue(), upgradeTypes.getOrDefault(entry.getKey(), FeatureUpdate.UpgradeType.UPGRADE), brokerFeatures, records));
        }

        if (validateOnly) {
            return ControllerResult.of(Collections.emptyList(), results);
        } else {
            return ControllerResult.atomicOf(records, results);
        }
    }

    MetadataVersion metadataVersion() {
        return metadataVersion.get();
    }

    ZkMigrationState zkMigrationState() {
        return migrationControlState.get();
    }

    private ApiError updateFeature(String featureName, short newVersion, FeatureUpdate.UpgradeType upgradeType, Map<Integer, Map<String, VersionRange>> brokersAndFeatures, List<ApiMessageAndVersion> records) {
        if (upgradeType.equals(FeatureUpdate.UpgradeType.UNKNOWN)) {
            return invalidUpdateVersion(featureName, newVersion, "The controller does not support the given upgrade type.");
        }

        final short currentVersion;
        if (featureName.equals(MetadataVersion.FEATURE_NAME)) {
            currentVersion = metadataVersion.get().featureLevel();
        } else {
            currentVersion = finalizedVersions.getOrDefault(featureName, (short) 0);
        }

        if (newVersion < 0) {
            return invalidUpdateVersion(featureName, newVersion, "A feature version cannot be less than 0.");
        }

        Optional<String> reasonNotSupported = quorumFeatures.reasonNotSupported(featureName, newVersion);
        if (reasonNotSupported.isPresent()) {
            return invalidUpdateVersion(featureName, newVersion, reasonNotSupported.get());
        }

        for (Entry<Integer, Map<String, VersionRange>> brokerEntry : brokersAndFeatures.entrySet()) {
            VersionRange brokerRange = brokerEntry.getValue().get(featureName);
            if (brokerRange == null) {
                return invalidUpdateVersion(featureName, newVersion, "Broker " + brokerEntry.getKey() + " does not support this feature.");
            } else if (!brokerRange.contains(newVersion)) {
                return invalidUpdateVersion(featureName, newVersion, "Broker " + brokerEntry.getKey() + " does not support the given " + "version. It supports " + brokerRange.min() + " to " + brokerRange.max() + ".");
            }
        }

        if (newVersion < currentVersion) {
            if (upgradeType.equals(FeatureUpdate.UpgradeType.UPGRADE)) {
                return invalidUpdateVersion(featureName, newVersion, "Can't downgrade the version of this feature without setting the " + "upgrade type to either safe or unsafe downgrade.");
            }
        } else if (newVersion > currentVersion) {
            if (!upgradeType.equals(FeatureUpdate.UpgradeType.UPGRADE)) {
                return invalidUpdateVersion(featureName, newVersion, "Can't downgrade to a newer version.");
            }
        }

        if (featureName.equals(MetadataVersion.FEATURE_NAME)) {
            // Perform additional checks if we're updating metadata.version
            return updateMetadataVersion(newVersion, upgradeType.equals(FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE), records::add);
        } else {
            records.add(new ApiMessageAndVersion(new FeatureLevelRecord().setName(featureName).setFeatureLevel(newVersion), (short) 0));
            return ApiError.NONE;
        }
    }

    private ApiError invalidUpdateVersion(String feature, short version, String message) {
        String errorMessage = String.format("Invalid update version %d for feature %s. %s", version, feature, message);
        log.debug(errorMessage);
        return new ApiError(Errors.INVALID_UPDATE_VERSION, errorMessage);
    }

    /**
     * Perform some additional validation for metadata.version updates.
     */
    private ApiError updateMetadataVersion(short newVersionLevel, boolean allowUnsafeDowngrade, Consumer<ApiMessageAndVersion> recordConsumer) {
        MetadataVersion currentVersion = metadataVersion();
        ZkMigrationState zkMigrationState = zkMigrationState();
        final MetadataVersion newVersion;
        try {
            newVersion = MetadataVersion.fromFeatureLevel(newVersionLevel);
        } catch (IllegalArgumentException e) {
            return invalidMetadataVersion(newVersionLevel, "Unknown metadata.version.");
        }

        // Don't allow metadata.version changes while we're migrating
        if (zkMigrationState.inProgress()) {
            return invalidMetadataVersion(newVersionLevel, "Unable to modify metadata.version while a " + "ZK migration is in progress.");
        }

        // We cannot set a version earlier than IBP_3_3_IV0, since that was the first version that contained
        // FeatureLevelRecord itself.
        if (newVersion.isLessThan(minimumBootstrapVersion)) {
            return invalidMetadataVersion(newVersionLevel, "Unable to set a metadata.version less than " + minimumBootstrapVersion);
        }
        if (newVersion.isLessThan(currentVersion)) {
            // This is a downgrade
            boolean metadataChanged = MetadataVersion.checkIfMetadataChanged(currentVersion, newVersion);
            if (!metadataChanged) {
                log.info("Downgrading metadata.version from {} to {}.", currentVersion, newVersion);
            } else if (allowUnsafeDowngrade) {
                return invalidMetadataVersion(newVersionLevel, "Unsafe metadata downgrade is not supported " + "in this version.");
            } else {
                return invalidMetadataVersion(newVersionLevel, "Refusing to perform the requested " + "downgrade because it might delete metadata information. Retry using " + "UNSAFE_DOWNGRADE if you want to force the downgrade to proceed.");
            }
        } else {
            log.info("Upgrading metadata.version from {} to {}.", currentVersion, newVersion);
        }

        recordConsumer.accept(new ApiMessageAndVersion(new FeatureLevelRecord().setName(MetadataVersion.FEATURE_NAME).setFeatureLevel(newVersionLevel), FEATURE_LEVEL_RECORD.lowestSupportedVersion()));

        return ApiError.NONE;
    }

    private ApiError invalidMetadataVersion(short version, String message) {
        String errorMessage = String.format("Invalid metadata.version %d. %s", version, message);
        log.error(errorMessage);
        return new ApiError(Errors.INVALID_UPDATE_VERSION, errorMessage);
    }

    FinalizedControllerFeatures finalizedFeatures(long epoch) {
        Map<String, Short> features = new HashMap<>();
        features.put(MetadataVersion.FEATURE_NAME, metadataVersion.get(epoch).featureLevel());
        for (Entry<String, Short> entry : finalizedVersions.entrySet(epoch)) {
            features.put(entry.getKey(), entry.getValue());
        }
        return new FinalizedControllerFeatures(features, epoch);
    }

    /**
     * Tests if the controller should be preventing metadata updates due to being in the PRE_MIGRATION
     * state. If the controller does not yet support migrations (before 3.4-IV0), then the migration state
     * will be NONE and this will return false. Once the controller has been upgraded to a version that supports
     * migrations, then this method checks if the migration state is equal to PRE_MIGRATION.
     */
    boolean inPreMigrationMode() {
        return migrationControlState.get().equals(ZkMigrationState.PRE_MIGRATION);
    }

    public void replay(FeatureLevelRecord record) {
        VersionRange range = quorumFeatures.localSupportedFeature(record.name());
        if (!range.contains(record.featureLevel())) {
            throw new RuntimeException("Tried to apply FeatureLevelRecord " + record + ", but this controller only " + "supports versions " + range);
        }
        if (record.name().equals(MetadataVersion.FEATURE_NAME)) {
            MetadataVersion mv = MetadataVersion.fromFeatureLevel(record.featureLevel());
            metadataVersion.set(mv);
            log.info("Replayed a FeatureLevelRecord setting metadata version to {}", mv);
        } else {
            if (record.featureLevel() == 0) {
                finalizedVersions.remove(record.name());
                log.info("Replayed a FeatureLevelRecord removing feature {}", record.name());
            } else {
                finalizedVersions.put(record.name(), record.featureLevel());
                log.info("Replayed a FeatureLevelRecord setting feature {} to {}", record.name(), record.featureLevel());
            }
        }
    }

    public void replay(ZkMigrationStateRecord record) {
        ZkMigrationState newState = ZkMigrationState.of(record.zkMigrationState());
        ZkMigrationState previousState = migrationControlState.get();
        if (previousState.equals(newState)) {
            log.debug("Replayed a ZkMigrationStateRecord which did not alter the state from {}.", previousState);
        } else {
            migrationControlState.set(newState);
            log.info("Replayed a ZkMigrationStateRecord changing the migration state from {} to {}.", previousState, newState);
        }
    }

    boolean isControllerId(int nodeId) {
        return quorumFeatures.isControllerId(nodeId);
    }
}
