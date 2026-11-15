# sub6 —— 每 6 小时自动更新的节点订阅

一条线：**公开源 → 去重 → TCP 存活筛 → 仓库 → jsDelivr 订阅 → 手机 v2rayNG**。

跟 `daily.py`（每天一封邮件）的关系：**并行的第二条线，互不干扰**。
daily 是"看一眼有什么"，sub6 是"手机直接订"，可以只要一个。

## 漏斗实测（2026-11-15，本机）

```
5 个公开源 7169 条
  → 去重 5464
  → TCP 存活 1643（30.1%）
  → 出订阅 600 条
全程 47 秒
```

## 部署（三步）

1. `sub6.py` 放到仓库根目录。
2. `sub6.yml` 的内容放到 `.github/workflows/sub6.yml`。
3. `Settings → Actions → General → Workflow permissions` 选
   **Read and write permissions**（workflow 里已带 `permissions: contents: write`，
   如果组织策略锁了全局只读，这一步就是必须的）。

第一次可以进 Actions 页手动点一次 `Run workflow` 验证。

## 订阅链接

```
https://cdn.jsdelivr.net/gh/你的用户名/你的仓库@main/out6/sub.txt
```

- `main` 换成你仓库的默认分支名（`main` 或 `master`）
- 仓库必须是**公开**的，jsDelivr 只服务公开仓库
- 每次跑完 workflow 会调 `purge.jsdelivr.net` 清缓存，一般 1 分钟内生效

## v2rayNG 配置

1. 左上角菜单 → **订阅分组设置** → 右上角 **+**
   - 备注：`sub6`
   - URL：上面那个 jsDelivr 链接
   - **自动更新**：打开，间隔选 **6 小时**
2. 回主界面 → 三点菜单 → **更新订阅**
3. 更新完点菜单里的 **测试全部配置真连接**（这一步最重要）

## 为什么"测速"只能做一半

| 在哪测 | 能测出什么 |
|---|---|
| CI（境外出口） | 节点**服务器活没活**（TCP 连不上 = 真死，直接扔） |
| 手机（你的网络） | **能不能翻墙**（v2rayNG 真连接测试） |

被墙的节点在境外照样连得上，所以 CI 只能做存活筛、做不了翻墙测试。
这一步的价值在于：公开源里七成是死节点，筛掉之后订阅体积小、手机测速快。

## 已知局限

- **公开节点寿命以天到周计**，订阅里随时有一批是当天就死的，正常。
- **jsDelivr 在大陆偶尔被干扰**，打不开就把订阅链接换成下述备选。
- **每 6 小时才更新一次**，急性子就手动点一次 Actions 里的 Run workflow。
- 提交由 bot 完成，仓库会有大量 commit；不想看就把 out6/ 加进 .gitignore 的
  反面——**不能加**，加了就没订阅了。

## 备选：jsDelivr 被墙时的中继

用现成的 Cloudflare Worker 反代仓库 raw 内容（Worker 访问 GitHub 不受墙）：

```js
export default {
  async fetch(req) {
    const u = new URL(req.url);
    const raw = "https://raw.githubusercontent.com/你的用户名/你的仓库/main" + u.pathname;
    const r = await fetch(raw, { cf: { cacheTtl: 300 } });
    return new Response(r.body, {
      headers: { "content-type": r.headers.get("content-type") || "text/plain", "access-control-allow-origin": "*" },
    });
  },
};
```

绑到自有域名后，订阅链接变成 `https://你的域名/out6/sub.txt`。
注意 Worker 域名在国内的可达性要实测，workers.dev 段被封过，必须挂自有 zone。

## 本地手动跑

```bash
python3 sub6.py                      # 全流程，产物在 out6/
python3 sub6.py --no-screen          # 跳过 TCP 筛
python3 sub6.py --limit 300          # 只留 300 条
python3 sub6.py --timeout 2 --concurrency 500
```
