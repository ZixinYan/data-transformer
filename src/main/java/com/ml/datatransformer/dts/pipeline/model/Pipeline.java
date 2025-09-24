package com.ml.datatransformer.dts.pipeline.model;

import java.util.List;

public interface Pipeline<I, O> {
    O execute(I input);
    List<Stage> stages();
} 