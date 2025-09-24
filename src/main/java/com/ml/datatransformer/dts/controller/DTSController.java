package com.ml.datatransformer.dts.controller;

import com.ml.datatransformer.dts.dto.DTSRequest;
import com.ml.datatransformer.dts.dto.DTSResponse;
import com.ml.datatransformer.dts.service.DTSService;
import com.ml.datatransformer.common.ApiResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 *  数据迁移任务接口
 */
@RestController
@RequestMapping("/api/aggregation")
@RequiredArgsConstructor
public class DTSController {

    private final DTSService dtsService;

    @PostMapping("/execute")
    public ApiResponse<DTSResponse> execute(@Valid @RequestBody DTSRequest request) {
        return ApiResponse.success(dtsService.execute(request));
    }
} 