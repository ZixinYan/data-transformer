package com.ml.datatransformer.dts.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 请求体
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DTSRequest {
    @NotBlank(message = "ruleId 不能为空")
    private String ruleId;

    private String payload;
} 