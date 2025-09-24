package com.ml.datatransformer.dts.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 响应
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DTSResponse {
    private String ruleId;
    private String result;
} 