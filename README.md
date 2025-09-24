# Data Transformer
---
### 项目简介
一个规则驱动的数据处理与发布系统，通过 YAML 描述任务，运行期按规则动态装配算子，并以虚拟线程（或回退普通线程）并发处理批数据。

### 目录结构（核心模块）
- `/pipeline/model`: 流水线通用接口 `Pipeline/Collector/Processor/Publisher/Stage`、`MapProcessor`
- `/pipeline`: 流水线实现
  - `DTSPipeline`: 单条数据串联执行（I→T→O）
  - `DTSPipelineBatch`: 批数据并发执行（I→List<M>→List<R>）
- `pipeline/impl`: 工厂与执行器
  - `OperatorFactory`: 按 type+config 创建算子
  - `DTSExecutor`: 虚拟线程执行器（回退功能暂时有问题，不影响使用）
- `pipeline/impl/*`: 各层算子（collect/clean/calculate/publish）
- `pipeline/task`: 任务加载与注册
  - `YamlTaskLoader/TaskAutoLoader/TaskRegistry/TaskDefinition/FlowDefinition/OperatorStep`
- `src/main/java/com/ml/datatransforemer/dts/service`: 服务层
  - `DTSService/DTSServiceImpl`: 解析任务 → 调 `OperatorFactory` → 组装 Pipeline → 用 `DTSExecutor` 执行
- `src/main/resources/task.yaml`: 任务配置示例

### 配置
在 `application.yml` 配置任务文件路径和数据库：
```
dts:
  taskPath: ${your path}
```

### 流程说明（Sequence Diagram）
[![流程图](https://www.plantuml.com/plantuml/png/ZLFDRYCj4Bpp52WdBDdZ-wTFKQI9BQraoIEF3xaIfksWnS024s_UVXiFGs2zF-iJGrLNTNNZ9n-u2-D54Iw2SRHL4dGW5Y-ba9RhG5kZWpDAWQFSqxR2Ud3lKa04-WeuSl_r06VfW_k8wF5GmNiBZgFVDhfEZFssOdJIWfBw9jtjAkZvs6--WnYnCgARbmfzQ6ElKTQlEsS4U7yxZgzA-ZTm3pUphYTaoafNZxIb_z5ktn_fcbkv7e9Vmyq0A4Ep0tmRmOT5795yvtZROd6iwx4kCPcxOw55ZopCZhGp1EP61L-6We5JGN2LbEBCdk4ajGpIQ2Qc74jwKkRg5sI0eYv9xxTO9npWPOxDmeU59HL6d-IvCCC18tqsotJKBIZtkfypPAcHtoZkf0__P_RZNtNJGlwntqp_fMwtGug6QtmRyyjXOewuf3Q7mFEiL7XrkoIqUK4XFxvUP60s2SMz55nyRmHfoJNlVq7mCqRiOhlPkqadrZHD8O8i4fchMSRnAnruu5Ky915bZALRuoZUltq68HH_fMD6cdIQ72Ej4emNbNEYmp3vH_od7a0Ff4gNHzYa4tFWHnN-530nSRWYsjtbZFhp0YQrae8BGcH-3FSuBcNoQX6-_MUzDTf3RgpWzkL7IvumtdXHFm00)

- 请求示例（DTSRequest）：
```
{
  "ruleId": "aggregation-task",
  "payload": "{\"scheme_rate\":0.01,\"interchange_rate\":0.02,\"markup_rate\":0.03,\"scheme_fee\":10,\"interchange_fee\":20,\"markup_fee\":30,\"amount_usd\":1000,\"amount\":1200}"
}
```

### 运行
- 启动：`mvn spring-boot:run`
- 接口：`POST /api/dts/execute`（Body 为 DTSRequest）


### 未来优化
- 流控与重试：对单条处理失败的策略（continue/stop/retry），配合监控发布。
- 热更新：新增 `ReloadController` 提供 `/api/dts/tasks/reload` 触发 `YamlTaskLoader.load()`。
- 类型通道统一：约定上下文键（如 `rows/payload/ruleId`），便于算子通信。

### 其他说明
 - 这个项目现网原本是go语言，这里为了开源使用Java重写，目前还未在现网进行运行
 - 项目核心设计大致如readme所示，有需要可以在未来进行自行拓展
