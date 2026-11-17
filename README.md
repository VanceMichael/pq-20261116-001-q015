# 滚动新闻快照采集与归档

本服务面向新闻编辑部和数据运营人员：采集经常刷新的新闻目录页面，规范化标题与链接，并在本地 SQLite 中保留每次采集及文章出现历史。一次短暂的页面缺项不会立刻从归档删除新闻；操作者可以查看快照差异、采集健康、审核记录，并导出 CSV。

## 环境

- Node.js `22.23.4`。
- 仅使用 Node 内置模块和 SQLite，无第三方包，不要求网络连接。

## 验收与演示

```sh
npm test
npm run build
npm run demo
```

演示会读取本地合成页面并在临时目录建立数据库，输出一次采集摘要后清理临时文件。生产数据库由 `ARCHIVE_DB` 指定，默认 `./var/archive.sqlite`。默认 HTTP 监听地址为 `127.0.0.1:8080`，可用 `HOST` 和 `PORT` 调整。

## HTTP 接口

- `POST /v1/feeds`：注册来源，提交 `feedId`、`baseUrl`、`allowedHosts`，以及可选的 `minimumItems`、`missingThreshold`。
- `POST /v1/feeds/:feedId/snapshots`：提交已保存的 HTML 页面；可用 `X-Captured-At` 指定采集时间。
- `POST /v1/feeds/:feedId/refresh`：手动抓取已注册来源；仅访问允许列表中的 DNS 主机。
- `GET /v1/feeds/:feedId`：查看 feed 状态、文章统计和采集健康。
- `GET /v1/feeds/:feedId/items`：分页读取文章；`visibility=visible|quiet` 可过滤状态。
- `GET /v1/feeds/:feedId/runs`：读取近期页面快照接纳/拒绝记录。
- `GET /v1/feeds/:feedId/attempts`：读取抓取请求、重试、失败和响应审计。
- `GET /v1/feeds/:feedId/changes`：比较最近两次已接纳快照；未达到两次时返回 `409`。
- `POST /v1/feeds/:feedId/delivery/:consumerId`：注册可续取的变更消费者，`startAt` 为 `earliest`（默认）或 `latest`；重复注册不重置已有游标。
- `GET /v1/feeds/:feedId/delivery/:consumerId?limit=`：按消费者自身游标读取不可变的已交付事件，事件类型为 `observed`（已接纳观测，含 `firstObserved`）、`field_corrected`（标题/发布时间更正）和 `omitted`（列表遗漏，附“遗漏不代表来源删除”说明）。读取不推进游标。
- `POST /v1/feeds/:feedId/delivery/:consumerId/checkpoint`：以 `{ "seq": N }` 显式确认进度；只能前进，重复确认返回 `unchanged`，超过最新序号返回 `409`。
- `POST /v1/feeds/:feedId/delivery/retention`：按保留期（默认 7 天，可用 `retentionDays` 配置）清理过期事件，返回保留序号下限和受影响的消费者。
- `GET /v1/feeds/:feedId/health`：读取 24 小时抓取成功率、最近成功时间和异常提示。
- `GET /v1/feeds/:feedId/items.csv`：流式导出；限制输出行数并转义表格公式前缀。
- `PATCH /v1/feeds/:feedId/items/:articleId/review`：按修订号更新审核状态，状态为 `unreviewed`、`confirmed` 或 `withdrawn`。
- `GET /v1/feeds/:feedId/items/:articleId/review`：查看审核事件历史。
- `GET /v1/feeds/:feedId/items/:articleId/history`：查看文章在已接纳列表快照中的出现位置和间隔。
- `GET /health`：进程健康检查。

抓取失败、页面异常和正常列表变更有不同记录。相同页面指纹重复提交时不会重复增加文章观察次数。短暂缺失需达到来源配置的连续快照阈值后才转为 `quiet`，重新出现时恢复 `visible`。人工审核状态独立于可见性状态，旧修订号会得到冲突响应而不会覆盖新审核决定。

### 可续取交付的保证

- 交付事件只来自**已接纳**快照：HTTP 抓取成功但被质量规则拒绝的页面不会进入 `delivery_inbox`，也不会产生任何变更事件。
- 事件在生成时冻结为 JSON 载荷并只追加、不原地修改，因此重取同一页返回完全相同的内容；采集与审核在读取期间继续进行也不影响已交付内容。
- 每个消费者持有独立游标，读取不改变游标，只有 checkpoint 才推进；过期游标（早于保留窗口）返回 `410 cursor_expired`，绝不会静默跳到最新位置。
- 采集事务提交时，交付载荷在同一事务写入崩溃安全的 `delivery_inbox`。若进程在事务提交后、交付索引完成前退出，下次导入、读取或显式恢复（`recoverDelivery`）会幂等补齐，已提交的采集不会丢失也不会重复交付。
- 事件序号按来源单调递增，保留期清理只删除旧事件而不复用序号。

仓库中的 HTML 样本为本地合成数据，仅用于离线演示和测试。实际部署前请配置准确来源域名，并遵守来源站点的使用条件。本服务不在后台隐式启动定时抓取；由操作者或外部调度器显式触发刷新。
