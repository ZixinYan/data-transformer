package com.ml.datatransformer.dts.service;

import com.ml.datatransformer.dts.dto.DTSRequest;
import com.ml.datatransformer.dts.dto.DTSResponse;

/**
 * 聚合服务接口
 */
public interface DTSService {
    DTSResponse execute(DTSRequest request);
} 