/**
 * Amazon Kinesis Scaling Utility
 * <p>
 * Copyright 2014, Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * <p>
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * <p>
 * http://aws.amazon.com/asl/
 * <p>
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.services.kinesis.scaling.auto;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;

public class AutoscalingConfigurationBuilder implements Serializable {
    private static final Log LOG = LogFactory
            .getLog(AutoscalingConfigurationBuilder.class);

    private String streamName;

    private String region = "us-east-1";

    private KinesisOperationType scaleOnOperation;

    private ScalingConfig scaleUp;

    private ScalingConfig scaleDown;

    private Integer minShards;

    private Integer maxShards;

    private Integer refreshShardsNumberAfterMin = 10;

    public AutoscalingConfigurationBuilder withStreamName(String streamName) {
        this.streamName = streamName;
        return this;
    }

    public AutoscalingConfigurationBuilder withRegion(String region) {
        this.region = region;
        return this;
    }

    public AutoscalingConfigurationBuilder withScaleOnOperation(String scaleOnOperation) {
        this.scaleOnOperation = KinesisOperationType.valueOf(scaleOnOperation);
        return this;
    }

    public AutoscalingConfigurationBuilder withScaleUp(Integer scaleAfterMins, Integer coolOffMins, Integer scaleCount, Integer scaleThresholdPct, Integer scalePct, String notificationARN) {
        ScalingConfig scalingConfig = new ScalingConfig();
        scalingConfig.setScaleThresholdPct(scaleThresholdPct);
        scalingConfig.setScaleAfterMins(scaleAfterMins);
        scalingConfig.setScalePct(scalePct);
        if (scaleCount != null) {
            scalingConfig.setScaleCount(scaleCount);
        }
        scalingConfig.setNotificationARN(notificationARN);
        scalingConfig.setCoolOffMins(coolOffMins);
        this.scaleUp = scalingConfig;
        return this;
    }

    public AutoscalingConfigurationBuilder withScaleDown(Integer scaleAfterMins, Integer coolOffMins, Integer scaleCount, Integer scaleThresholdPct, Integer scalePct, String notificationARN) {
        ScalingConfig scalingConfig = new ScalingConfig();
        scalingConfig.setScaleThresholdPct(scaleThresholdPct);
        scalingConfig.setScaleAfterMins(scaleAfterMins);
        scalingConfig.setScalePct(scalePct);
        if (scaleCount != null) {
            scalingConfig.setScaleCount(scaleCount);
        }
        scalingConfig.setNotificationARN(notificationARN);
        scalingConfig.setCoolOffMins(coolOffMins);
        this.scaleDown = scalingConfig;
        return this;
    }

    public AutoscalingConfigurationBuilder withMinShards(Integer minShards) {
        this.minShards = minShards;
        return this;
    }

    public AutoscalingConfigurationBuilder withMaxShards(Integer maxShards) {
        this.maxShards = maxShards;
        return this;
    }


    public AutoscalingConfigurationBuilder withRefreshShardsNumberAfterMin(
            Integer refreshShardsNumberAfterMin) {
        this.refreshShardsNumberAfterMin = refreshShardsNumberAfterMin;
        return this;
    }

    public AutoscalingConfiguration build() {
        AutoscalingConfiguration autoscalingConfiguration = new AutoscalingConfiguration();
        try {
            autoscalingConfiguration.setStreamName(streamName);
            autoscalingConfiguration.setRegion(region);
            autoscalingConfiguration.setMinShards(minShards);
            autoscalingConfiguration.setMaxShards(maxShards);
            autoscalingConfiguration.setScaleOnOperation(scaleOnOperation);
            autoscalingConfiguration.setScaleUp(scaleUp);
            autoscalingConfiguration.setScaleDown(scaleDown);
            autoscalingConfiguration.setRefreshShardsNumberAfterMin(refreshShardsNumberAfterMin);
        } catch (Exception e) {
            throw new RuntimeException("Failed to init AutoscalingConfiguration");
        }
        return autoscalingConfiguration;

    }

}
