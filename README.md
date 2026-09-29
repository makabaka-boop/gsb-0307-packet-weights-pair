# packets — JSON 命令行调度器（SFQ / 起始时间公平排队）

多条数据流共用一条**非抢占**链路。按到达顺序发包会让大流量长期挤占小流；只按包长排序又可能让同一流的包乱序。本程序按**起始时间公平排队**调度：为每个包计算虚拟开始/完成标签，链路空闲时总是发送完成标签最小的队首包，同流包保持 FIFO。

## 调度规则

权重可在输入的 `weight_changes` 中按时间调整：每项含 `at`（非负整数时刻）、`stream` 和 `weight`（1～10）。数组按 `at` 非降序排列，同一流同刻至多一项；跨流同刻调整合法。一个时刻的完成、权重调整和到达均先于下一次选包；该时刻新到达的包使用新权重。权重调整还会改变该流已到达但尚未开始传输的积压包的标签及其与其他流的竞争顺序；重计价以调整时刻的虚拟时间和该流已开始传输的最后完成标签为起点，按同流 FIFO 连续计算。正在传输及已经完成的包保持原标签，传输不被中断。跨流同刻调整应与输入排列顺序无关。非法调整整份拒绝。

- 全局虚拟时间 `V` 与各流的"上一包完成标签"初值均为 `0`。
- 包到达时赋标签：`S = max(该流上一包的完成标签, 当前 V)`，`F = S + 时长/权重`（精确有理数）。
- 链路空闲时，从**已到达且位于各流队首**的包中按 `(F, 流 id, 包 id)` 升序选一个，开始传输并把 `V` 更新为该包的 `F`；传输不抢占。
- 同一时刻的到达**先于**选包处理；完成事件与到达同刻时，两者都先于选包。
- 队列全空且链路空闲时，时间直接跳到下一个到达时刻（空闲段计入输出）。

## 输入（stdin 或文件参数）

```json
{
  "streams": [
    {"id": "a", "weight": 2},
    {"id": "b", "weight": 1}
  ],
  "packets": [
    {"id": "p1", "stream": "a", "arrival": 0, "duration": 4}
  ]
}
```

约束（违反即报错退出）：

- 2～20 条流，`id` 为唯一非空可打印 ASCII（不含空白），`weight` ∈ [1,10]；
- 最多 500 个包，`id` 唯一，`stream` 必须已声明，`arrival` 为非负整数且整体非降序（同刻按输入次序，同流保持 FIFO），`duration` ∈ [1,1000]。

## 输出（stdout）

```json
{
  "packets": [
    {"id": "p1", "stream": "a", "arrival": 0, "duration": 4,
     "start_tag": "0", "finish_tag": "2", "start": 0, "end": 4}
  ],
  "schedule": ["p1"],
  "idle": [[8, 9]],
  "completion": {"a": ["p1"], "b": []}
}
```

- `packets`：按输入顺序，每包的**约分标签**（`start_tag`/`finish_tag`，`big.Rat` 约分有理数，如 `"3"`、`"101/10"`）与**真实起止时刻** `start`/`end`；
- `schedule`：实际发包顺序（包 id）；
- `idle`：链路空闲段 `[start, end)` 列表；
- `completion`：各流包的完成次序（同流 FIFO，按完成事件记录）。

## 本地运行

```sh
go build -o packets .
./packets < examples/input.json
./packets -o out.json examples/input.json   # 文件输入/输出
go test ./...                                # 测试
```

## Compose 运行

```sh
docker compose run --build --rm -T packets < examples/input.json
# 或挂载文件：
docker compose run --build --rm -v "$PWD/examples:/data:ro" packets /data/input.json
```

## 测试

- `TestGolden`：手算金样例——空队列跳时、不同权重并列、传输中晚到不抢占、完成瞬刻到达、完成标签并列按流 id 决胜；
- `TestDifferential` / `TestDifferentialWide`：主实现与逐事件参考模拟器（`referenceSchedule`，全表扫描 + 排序选包的不同结构）对拍 3000 组随机输入，外加 200 组满规格输入（20 流 / 500 包），并校验不变量（`F = S + D/w`、标签已约分、`S ≥ 同流前包 F`、工作守恒 `start = max(arrival, 前包 end)`、空闲段与忙段互补、同流完成次序等于输入次序）；
- `TestValidate` / `TestRunErrors`：输入约束与 CLI 错误路径。

## 结构

- `sched.go`：类型、输入校验、调度器 `schedule`；
- `main.go`：CLI（stdin/文件输入，stdout/文件输出）；
- `sched_test.go`：参考模拟器、金样例、对拍与不变量；
- `main_test.go`：CLI 测试；
- `Dockerfile` / `docker-compose.yml`：`packets` 服务镜像与编排。
