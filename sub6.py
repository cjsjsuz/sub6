#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sub6.py —— 6 小时订阅生成器（单文件、纯标准库）

干什么：
  1. 从 5 个公开源抓节点（jsDelivr 进，不走 raw.githubusercontent）
  2. 解析 vmess/vless/trojan/ss/hysteria2 + clash yaml，去重
  3. 做一轮 TCP 存活筛（境外出口只能测「服务器活没活」，测不了「能不能翻墙」）
  4. 出订阅三件 + 统计，交给 CI 提交回仓库，由 jsDelivr 吐给 v2rayNG

为什么要 TCP 筛：
  公开源七成以上是死节点（服务器下线）。CI 出口在境外，没有墙，
  TCP 连不上的基本是真死，筛掉能显著缩小订阅体积、减少手机端测速时间。
  但「TCP 能连」≠「能翻墙」——被墙的节点在境外照样连得上，
  真正的可用性只能在手机网络里测（v2rayNG 订阅更新后点一次真连接测试）。

用法：
    python3 sub6.py                      # 采集 + TCP 筛 + 出件到 out6/
    python3 sub6.py --no-screen          # 跳过 TCP 筛（全量出件）
    python3 sub6.py --limit 600          # 存活节点最多保留多少条
    python3 sub6.py --timeout 2.5        # TCP 超时秒数
    python3 sub6.py --concurrency 400    # TCP 并发数
"""
import argparse
import asyncio
import base64
import binascii
import json
import os
import re
import statistics
import sys
import time
import urllib.parse
import urllib.request

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

# ---------------- 订阅源 ----------------
# 2026-11 实测（jsDelivr）：5 源里 4 个稳定 200，Eternity 在仓库根目录。
SOURCES = [
    ("anaer",      "https://cdn.jsdelivr.net/gh/anaer/Sub@main/clash.yaml", "clash"),
    ("mahdibland", "https://cdn.jsdelivr.net/gh/mahdibland/V2RayAggregator@master/sub/sub_merge.txt", "auto"),
    ("Peasoft",    "https://cdn.jsdelivr.net/gh/Peasoft/NoMoreWalls@master/list_raw.txt", "auto"),
    ("Eternity",   "https://cdn.jsdelivr.net/gh/mahdibland/V2RayAggregator@master/Eternity.txt", "auto"),
    ("Pawdroid",   "https://cdn.jsdelivr.net/gh/Pawdroid/Free-servers@main/sub", "auto"),
]

TIMEOUT = 60
RETRY = 1
KEEP = 600          # 存活节点最多保留条数（v2rayNG 测太多会卡）
MIN_OK = 20         # 存活少于这个数就在报告里标异常

# ---------------- 地区识别 ----------------
FLAG = {
    "🇭🇰": "HK", "🇸🇬": "SG", "🇯🇵": "JP", "🇺🇸": "US", "🇰🇷": "KR",
    "🇬🇧": "GB", "🇩🇪": "DE", "🇳🇱": "NL", "🇨🇦": "CA", "🇫🇷": "FR",
    "🇹🇼": "TW", "🇮🇹": "IT", "🇨🇿": "CZ", "🇦🇺": "AU", "🇮🇳": "IN",
    "🇷🇺": "RU", "🇹🇷": "TR", "🇻🇳": "VN", "🇹🇭": "TH", "🇲🇾": "MY",
    "🇵🇭": "PH", "🇮🇩": "ID", "🇧🇷": "BR", "🇦🇷": "AR", "🇿🇦": "ZA",
    "🇨🇭": "CH", "🇸🇪": "SE", "🇫🇮": "FI", "🇳🇴": "NO", "🇩🇰": "DK",
    "🇵🇱": "PL", "🇺🇦": "UA", "🇮🇪": "IE", "🇪🇸": "ES", "🇦🇹": "AT",
}
WORDS = [
    ("香港", "HK"), ("HK", "HK"), ("Hong", "HK"),
    ("新加坡", "SG"), ("狮城", "SG"), ("SG", "SG"), ("Singapore", "SG"),
    ("日本", "JP"), ("东京", "JP"), ("大阪", "JP"), ("JP", "JP"), ("Japan", "JP"),
    ("美国", "US"), ("洛杉矶", "US"), ("圣何塞", "US"), ("US", "US"), ("United States", "US"),
    ("韩国", "KR"), ("首尔", "KR"), ("KR", "KR"),
    ("台湾", "TW"), ("TW", "TW"),
    ("英国", "GB"), ("伦敦", "GB"), ("GB", "GB"),
    ("德国", "DE"), ("法兰克福", "DE"), ("DE", "DE"),
    ("荷兰", "NL"), ("NL", "NL"),
    ("加拿大", "CA"), ("CA", "CA"),
    ("法国", "FR"), ("FR", "FR"),
    ("俄罗斯", "RU"), ("RU", "RU"),
    ("土耳其", "TR"), ("TR", "TR"),
    ("越南", "VN"), ("VN", "VN"),
    ("泰国", "TH"), ("TH", "TH"),
    ("马来", "MY"), ("MY", "MY"),
    ("印度", "IN"), ("IN", "IN"),
    ("巴西", "BR"), ("BR", "BR"),
    ("澳大利亚", "AU"), ("澳洲", "AU"), ("AU", "AU"),
    ("意大利", "IT"), ("IT", "IT"),
    ("瑞士", "CH"), ("CH", "CH"),
    ("Cloudflare", "CF"), ("CF", "CF"), ("cloudflare", "CF"),
]
ORDER = ["HK", "SG", "JP", "TW", "KR", "US", "GB", "DE", "NL", "CA", "CF", "??"]


def guess_region(tag: str) -> str:
    if not tag:
        return "??"
    for f, code in FLAG.items():
        if f in tag:
            return code
    for w, code in WORDS:
        if w in tag:
            return code
    return "??"


# ---------------- 抓取 ----------------
def fetch(url: str) -> bytes:
    last = None
    for i in range(RETRY + 1):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": UA,
                "Accept": "*/*",
                "Accept-Language": "zh-CN,zh;q=0.9",
            })
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                return r.read()
        except Exception as e:
            last = e
            if i < RETRY:
                time.sleep(2 * (i + 1))
    raise last


def maybe_b64(text: str) -> str:
    """整体 base64 的订阅解码；不是 base64 就原样返回。"""
    s = re.sub(r"\s+", "", text)
    if not s:
        return text
    if not re.fullmatch(r"[A-Za-z0-9+/=_\-]+", s):
        return text
    padded = s + "=" * (-len(s) % 4)
    for decoder in (base64.b64decode, base64.urlsafe_b64decode):
        try:
            out = decoder(padded).decode("utf-8", "ignore")
            if "://" in out:
                return out
        except (binascii.Error, ValueError):
            continue
    return text


# ---------------- 单节点解析 ----------------
def parse_vmess(link: str):
    body = link[len("vmess://"):].split("#")[0].strip()
    body = body + "=" * (-len(body) % 4)
    try:
        j = json.loads(base64.b64decode(body).decode("utf-8", "ignore"))
    except Exception:
        return None
    host, port = j.get("add"), j.get("port")
    uuid = j.get("id")
    if not (host and port and uuid):
        return None
    try:
        port = int(port)
    except (TypeError, ValueError):
        return None
    return {
        "proto": "vmess",
        "name": str(j.get("ps") or f"{host}:{port}"),
        "server": str(host),
        "port": port,
        "cred": str(uuid),
        "net": j.get("net") or "tcp",
        "tls": str(j.get("tls") or "").lower() in ("tls", "reality"),
        "host_hdr": j.get("host") or "",
        "path": j.get("path") or "/",
        "sni": j.get("sni") or j.get("host") or "",
        "aid": j.get("aid", 0),
        "raw": link,
    }


def _q(link: str) -> dict:
    if "?" not in link:
        return {}
    qs = link.split("?", 1)[1].split("#")[0]
    out = {}
    for kv in qs.split("&"):
        if "=" in kv:
            k, v = kv.split("=", 1)
            out[k] = urllib.parse.unquote(v)
    return out


def _tag(link: str) -> str:
    return urllib.parse.unquote(link.split("#", 1)[1]) if "#" in link else ""


def parse_uri(link: str):
    link = link.strip()
    if not link or "://" not in link:
        return None
    scheme = link.split("://", 1)[0].lower()
    tag = _tag(link)
    try:
        if scheme == "vmess":
            return parse_vmess(link)
        if scheme in ("vless", "trojan"):
            body = link.split("://", 1)[1].split("#")[0]
            cred, _, hostport = body.partition("@")
            if not hostport:
                return None
            q = _q(link)
            host = hostport.split("?")[0].rsplit(":", 1)[0].strip("[]")
            port = hostport.split("?")[0].rsplit(":", 1)[-1]
            if not port.isdigit():
                return None
            return {
                "proto": scheme,
                "name": tag or f"{host}:{port}",
                "server": host,
                "port": int(port),
                "cred": urllib.parse.unquote(cred),
                "net": q.get("type", "tcp"),
                "tls": q.get("security", "") in ("tls", "reality", "xtls"),
                "host_hdr": q.get("host", ""),
                "path": q.get("path", "/"),
                "sni": q.get("sni", "") or q.get("host", ""),
                "flow": q.get("flow", ""),
                "pbk": q.get("pbk", ""),
                "sid": q.get("sid", ""),
                "fp": q.get("fp", ""),
                "aid": 0,
                "raw": link,
            }
        if scheme == "ss":
            body = link.split("://", 1)[1].split("#")[0]
            if "@" in body:
                cred_b64, hostport = body.rsplit("@", 1)
                try:
                    cred = base64.urlsafe_b64decode(
                        cred_b64 + "=" * (-len(cred_b64) % 4)).decode("utf-8", "ignore")
                except Exception:
                    cred = cred_b64
            else:
                dec = base64.urlsafe_b64decode(
                    body.split("?")[0] + "=" * (-len(body.split("?")[0]) % 4)
                ).decode("utf-8", "ignore")
                cred, _, hostport = dec.rpartition("@")
            host = hostport.rsplit(":", 1)[0].strip("[]")
            port = hostport.rsplit(":", 1)[-1]
            if not port.isdigit():
                return None
            return {
                "proto": "ss",
                "name": tag or f"{host}:{port}",
                "server": host, "port": int(port), "cred": cred,
                "net": "tcp", "tls": False, "host_hdr": "", "path": "",
                "sni": "", "aid": 0, "raw": link,
            }
        if scheme in ("hysteria2", "hy2"):
            # 先切 fragment 与 query 再切 @：obfs-password 里常含 @
            body = link.split("://", 1)[1].split("#", 1)[0].split("?", 1)[0]
            cred, _, hostport = body.rpartition("@")
            if not hostport:
                return None
            q = _q(link)
            hp = hostport.rstrip("/")
            host = hp.rsplit(":", 1)[0].strip("[]")
            port = hp.rsplit(":", 1)[-1]
            if not port.isdigit():
                return None
            return {
                "proto": "hysteria2",
                "name": tag or f"{host}:{port}",
                "server": host, "port": int(port),
                "cred": urllib.parse.unquote(cred),
                "net": "quic", "tls": True, "host_hdr": "", "path": "",
                "sni": q.get("sni", "") or q.get("peer", ""),
                "aid": 0, "raw": link,
                "obfs": (q.get("obfs") or "").lower(),
                "obfs_password": q.get("obfs-password") or q.get("obfs_password") or "",
            }
    except Exception:
        return None
    return None


def parse_clash_yaml(text: str):
    """极简 clash yaml 解析：只抠 proxies 段，不依赖 pyyaml。"""
    out = []
    cur = None
    in_proxies = False
    ws_mode = False
    sub_key = None

    def flush():
        nonlocal cur, ws_mode, sub_key
        c, cur = cur, None
        ws_mode, sub_key = False, None
        if not c:
            return
        t = str(c.get("type", "")).lower()
        if t not in ("vmess", "vless", "trojan", "ss", "http", "https"):
            return
        try:
            port = int(str(c.get("port", 0)).strip())
        except (TypeError, ValueError):
            return
        server = str(c.get("server", "")).strip()
        if not server or not port:
            return
        cred = c.get("uuid") or c.get("password") or ""
        if t == "ss":
            cred = f"{c.get('cipher', '')}:{c.get('password', '')}"
        sni = c.get("servername") or c.get("sni") or c.get("ws-host") or ""
        out.append({
            "proto": t,
            "name": str(c.get("name", "")).strip(),
            "server": server,
            "port": port,
            "cred": str(cred),
            "net": str(c.get("network", "tcp") or "tcp"),
            "tls": str(c.get("tls", "")).lower() in ("true", "1"),
            "host_hdr": str(c.get("ws-host", "") or sni),
            "path": str(c.get("ws-path", "/") or "/"),
            "sni": str(sni),
            "aid": c.get("alterId", 0),
            "pbk": str(c.get("pbk", "") or ""),
            "sid": str(c.get("sid", "") or ""),
            "raw": None,
        })

    for raw in text.splitlines():
        line = raw.rstrip()
        if not line or line.lstrip().startswith("#"):
            continue
        if re.match(r"^[A-Za-z_][\w-]*:", line):
            key = line.split(":", 1)[0]
            if key == "proxies":
                in_proxies = True
            elif in_proxies:
                flush()
                in_proxies = False
            continue
        if not in_proxies:
            continue

        mflow = re.match(r"^\s*-\s*\{(.*)\}\s*$", line)
        if mflow:
            flush()
            c = {}
            for kv in mflow.group(1).split(","):
                if ":" in kv:
                    k, v = kv.split(":", 1)
                    c[k.strip()] = v.strip().strip("\"'")
            cur = c
            flush()
            continue

        mstart = re.match(r"^\s*-\s+([A-Za-z_][\w-]*):\s*(.*)$", line)
        if mstart:
            flush()
            cur = {mstart.group(1): mstart.group(2).strip().strip("\"'")}
            continue

        if cur is None:
            continue

        mk = re.match(r"^\s+([A-Za-z_][\w-]*):\s*(.*)$", line)
        if not mk:
            continue
        k, v = mk.group(1), mk.group(2).strip().strip("\"'")

        if k in ("ws-opts", "reality-opts", "grpc-opts", "h2-opts"):
            ws_mode = True
            sub_key = None
            continue
        if ws_mode:
            if not v:
                sub_key = k
                continue
            if sub_key == "headers" and k.lower() in ("host", "hosts"):
                cur["ws-host"] = v
                continue
            if k == "path":
                cur["ws-path"] = v
                continue
            if k == "public-key":
                cur["pbk"] = v
                continue
            if k == "short-id":
                cur["sid"] = v
                continue
            continue
        cur[k] = v

    flush()
    return out


# ---------------- 反向出件 ----------------
def to_uri(n: dict) -> str:
    if n.get("raw"):
        return n["raw"]
    up = urllib.parse
    tag = up.quote(n["name"] or f"{n['server']}:{n['port']}", safe="")
    if n["proto"] == "vmess":
        j = {"v": "2", "ps": n["name"], "add": n["server"], "port": str(n["port"]),
             "id": n["cred"], "aid": str(n.get("aid", 0)), "scy": "auto",
             "net": n["net"], "type": "none", "host": n.get("host_hdr", ""),
             "path": n.get("path", "/"), "tls": "tls" if n["tls"] else "",
             "sni": n.get("sni", "")}
        return "vmess://" + base64.b64encode(
            json.dumps(j, ensure_ascii=False).encode()).decode()
    if n["proto"] in ("vless", "trojan"):
        q = [f"security={'tls' if n['tls'] else 'none'}"]
        if n.get("sni"):
            q.append(f"sni={up.quote(n['sni'], safe='')}")
        q.append(f"type={n['net']}")
        if n["net"] == "ws":
            if n.get("host_hdr"):
                q.append(f"host={up.quote(n['host_hdr'], safe='')}")
            q.append(f"path={up.quote(n.get('path', '/'), safe='')}")
        if n.get("flow"):
            q.append(f"flow={n['flow']}")
        if n.get("fp"):
            q.append(f"fp={n['fp']}")
        if n.get("pbk"):
            q.append(f"pbk={up.quote(n['pbk'], safe='')}")
            q.append(f"sid={up.quote(n.get('sid', ''), safe='')}")
        cred = up.quote(n["cred"], safe="")
        return f"{n['proto']}://{cred}@{n['server']}:{n['port']}?{'&'.join(q)}#{tag}"
    if n["proto"] == "ss":
        cred = base64.urlsafe_b64encode(n["cred"].encode()).decode().rstrip("=")
        return f"ss://{cred}@{n['server']}:{n['port']}#{tag}"
    if n["proto"] == "hysteria2":
        return n["raw"]
    return ""


def to_clash(n: dict) -> dict:
    d = {"name": n["name"][:40], "type": n["proto"], "server": n["server"],
         "port": n["port"], "udp": False}
    if n["proto"] == "vmess":
        d.update({"uuid": n["cred"], "alterId": n.get("aid", 0),
                  "cipher": "auto", "tls": n["tls"]})
    elif n["proto"] == "vless":
        d.update({"uuid": n["cred"], "tls": n["tls"], "flow": n.get("flow", "")})
    elif n["proto"] == "trojan":
        d.update({"password": n["cred"], "tls": True})
    elif n["proto"] == "ss":
        cipher, _, pw = n["cred"].partition(":")
        d.update({"cipher": cipher or "aes-256-gcm", "password": pw or n["cred"]})
    elif n["proto"] == "hysteria2":
        d.update({"password": n["cred"],
                  "sni": n.get("sni") or n["server"],
                  "skip-cert-verify": True})
        d["udp"] = True
        if n.get("obfs"):
            d["obfs"] = n["obfs"]
            if n.get("obfs_password"):
                d["obfs-password"] = n["obfs_password"]
    if n["net"] == "ws":
        d["network"] = "ws"
        d["ws-opts"] = {"path": n.get("path") or "/",
                        "headers": {"Host": n.get("host_hdr") or n.get("sni") or n["server"]}}
    if n.get("sni") and n["proto"] != "hysteria2":
        d["servername"] = n["sni"]
    if n.get("fp"):
        d["client-fingerprint"] = n["fp"]
    if n["proto"] == "vless" and n.get("pbk"):
        d["reality-opts"] = {"public-key": n["pbk"], "short-id": n.get("sid", "")}
    return d


# ---------------- TCP 存活筛 ----------------
UDP_PROTOS = {"hysteria2", "tuic", "wireguard"}


async def _tcp_one(host: str, port: int, timeout: float):
    t0 = time.monotonic()
    w = None
    try:
        fut = asyncio.open_connection(host, port)
        _r, w = await asyncio.wait_for(fut, timeout)
        ms = int((time.monotonic() - t0) * 1000)
        return True, ms
    except Exception:
        return False, 0
    finally:
        if w is not None:
            try:
                w.close()
            except Exception:
                pass


async def _screen_impl(nodes, timeout, concurrency):
    sem = asyncio.Semaphore(concurrency)
    results = [None] * len(nodes)

    async def one(i, n):
        async with sem:
            ok, ms = await _tcp_one(n["server"], n["port"], timeout)
            results[i] = (ok, ms)

    t0 = time.monotonic()
    await asyncio.gather(*(one(i, n) for i, n in enumerate(nodes)))
    print(f"    TCP 筛完成：{len(nodes)} 条，用时 {time.monotonic() - t0:.1f}s")
    return results


def tcp_screen(nodes, timeout=3.0, concurrency=300):
    """返回与 nodes 等长的 [(alive, ms)]；UDP 协议跳过筛，alive=None。"""
    screened, results = [], []
    for n in nodes:
        if n["proto"] in UDP_PROTOS:
            screened.append(n)
            results.append((None, 0))
        else:
            screened.append(n)
            results.append("pending")
    idx = [i for i, r in enumerate(results) if r == "pending"]
    if idx:
        sub = [screened[i] for i in idx]
        got = asyncio.run(_screen_impl(sub, timeout, concurrency))
        for j, i in enumerate(idx):
            results[i] = got[j]
    return results


# ---------------- 主流程 ----------------
def build(outdir: str, limit: int, timeout: float, concurrency: int, screen: bool):
    os.makedirs(outdir, exist_ok=True)
    t0 = time.time()

    # 1) 采集
    all_nodes, src_stats = [], []
    for name, url, kind in SOURCES:
        try:
            text = maybe_b64(fetch(url).decode("utf-8", "ignore"))
            if kind == "clash" or text.lstrip().startswith(("port:", "mixed-port:", "proxies:")):
                got = parse_clash_yaml(text)
            else:
                got = [n for n in (parse_uri(x) for x in text.splitlines()) if n]
            for n in got:
                n["source"] = name
                n["region"] = guess_region(n["name"])
            src_stats.append({"name": name, "count": len(got), "status": "ok"})
            print(f"  [{name}] {len(got)}")
            all_nodes.extend(got)
        except Exception as e:
            src_stats.append({"name": name, "count": 0,
                              "status": f"失败 {type(e).__name__}"})
            print(f"  [{name}] 失败 {type(e).__name__}")

    # 2) 合法性过滤
    good = []
    for n in all_nodes:
        s = (n.get("server") or "").strip()
        if not s or " " in s or s in ("127.0.0.1", "0.0.0.0", "localhost"):
            continue
        if not (0 < int(n.get("port") or 0) < 65536):
            continue
        if not n.get("cred"):
            continue
        good.append(n)

    # 3) 去重（协议 + 端点 + 凭据）
    seen, uniq = set(), []
    for n in good:
        key = (n["proto"], n["server"].lower(), n["port"], n["cred"][:32])
        if key in seen:
            continue
        seen.add(key)
        uniq.append(n)

    # 多源命中次数
    cnt = {}
    for n in good:
        k = (n["proto"], n["server"].lower(), n["port"])
        cnt[k] = cnt.get(k, 0) + 1
    for n in uniq:
        n["hits"] = cnt[(n["proto"], n["server"].lower(), n["port"])]

    # 4) TCP 存活筛
    alive_n = None
    if screen:
        print(f"  TCP 筛：{len(uniq)} 条，超时 {timeout}s，并发 {concurrency}")
        res = tcp_screen(uniq, timeout, concurrency)
        kept_c = []
        for n, (ok, ms) in zip(uniq, res):
            n["alive"] = ok
            n["ms"] = ms
            if ok is True:
                kept_c.append(n)
            elif ok is None:      # UDP 协议（hy2/tuic），筛不了，保留
                kept_c.append(n)
        alive_n = sum(1 for n, (ok, _) in zip(uniq, res) if ok is True)
        print(f"  TCP 存活 {alive_n} / {len(uniq)}"
              f"（{alive_n / max(1, len(uniq)) * 100:.1f}%），UDP 免筛 "
              f"{len(kept_c) - alive_n}")
        uniq = kept_c
    else:
        for n in uniq:
            n["alive"] = None
            n["ms"] = 0

    # 5) 排序：多源命中 > 延迟（CI 视角，仅供参考）> 地区 > 协议
    uniq.sort(key=lambda n: (
        -n["hits"],
        n["ms"] if n["ms"] else 99999,
        ORDER.index(n["region"]) if n["region"] in ORDER else 99,
        n["proto"],
    ))
    kept = uniq[:limit]

    # 6) 出件：base64 订阅 / 明文 / clash / stats
    links = [u for u in (to_uri(n) for n in kept) if u]
    sub_path = os.path.join(outdir, "sub.txt")
    with open(sub_path, "w", encoding="utf-8") as f:
        f.write(base64.b64encode("\n".join(links).encode()).decode())

    links_path = os.path.join(outdir, "links.txt")
    with open(links_path, "w", encoding="utf-8") as f:
        f.write("\n".join(links) + "\n")

    proxies, seen_nm = [], set()
    for n in kept:
        p = to_clash(n)
        base, i = p["name"], 2
        while p["name"] in seen_nm:
            p["name"] = f"{base[:36]}-{i}"
            i += 1
        seen_nm.add(p["name"])
        proxies.append(p)
    names = [p["name"] for p in proxies]
    y = ["mixed-port: 7890", "allow-lan: false", "mode: rule", "log-level: warning",
         "proxies:"]
    for p in proxies:
        y.append("  - " + json.dumps(p, ensure_ascii=False))
    y += ["proxy-groups:",
          "  - name: 自动选择", "    type: url-test",
          "    url: http://www.gstatic.com/generate_204",
          "    interval: 300", "    tolerance: 50",
          "    proxies: " + json.dumps(names, ensure_ascii=False),
          "  - name: 手动", "    type: select",
          "    proxies: " + json.dumps(["自动选择", "DIRECT"] + names, ensure_ascii=False),
          "rules:", "  - GEOIP,LAN,DIRECT", "  - GEOIP,CN,DIRECT", "  - MATCH,手动"]
    clash_path = os.path.join(outdir, "clash.yaml")
    with open(clash_path, "w", encoding="utf-8") as f:
        f.write("\n".join(y) + "\n")

    # 统计
    by_region, by_proto = {}, {}
    for n in kept:
        by_region[n["region"]] = by_region.get(n["region"], 0) + 1
        by_proto[n["proto"]] = by_proto.get(n["proto"], 0) + 1
    mss = [n["ms"] for n in kept if n["ms"]]
    stats = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "sources": src_stats,
        "raw": len(all_nodes),
        "valid": len(good),
        "uniq": len(seen),
        "screened": bool(screen),
        "tcp_alive": alive_n,
        "kept": len(kept),
        "by_region": by_region,
        "by_proto": by_proto,
        "tcp_ms_median": int(statistics.median(mss)) if mss else None,
        "elapsed_s": round(time.time() - t0, 1),
        "files": ["sub.txt", "links.txt", "clash.yaml", "stats.json", "report.md"],
    }
    with open(os.path.join(outdir, "stats.json"), "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    # 人读报告
    reg = " ".join(f"{k}:{v}" for k, v in sorted(by_region.items(), key=lambda kv: -kv[1]))
    proto = " ".join(f"{k}:{v}" for k, v in sorted(by_proto.items(), key=lambda kv: -kv[1]))
    src = "\n".join(f"- {s['name']}: {s['count']} 条（{s['status']}）" for s in src_stats)
    report = f"""# 本轮订阅生成报告

生成时间：{stats['generated_at']}（北京时间）

## 漏斗

| 阶段 | 数量 |
|---|---|
| 原始解析 | {len(all_nodes)} |
| 合法 | {len(good)} |
| 去重 | {len(seen)} |
| TCP 存活 | {alive_n if alive_n is not None else '未筛'} |
| 最终保留 | {len(kept)} |

## 源状态

{src}

## 地区分布

{reg}

## 协议分布

{proto}

## 说明

- TCP 筛在境外出口执行，只能证明节点服务器还活着，**不能证明能翻墙**。
- 真正的可用性请在手机 v2rayNG 里订阅后执行一次「测试全部真连接」。
- 延迟数字是 CI 到节点的，与你的手机网络无关，只用于排序。
"""
    with open(os.path.join(outdir, "report.md"), "w", encoding="utf-8") as f:
        f.write(report)

    print(f"\n产物 → {outdir}/  用时 {time.time() - t0:.1f}s")
    print(report)
    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd(),
        "out6"))
    ap.add_argument("--limit", type=int, default=KEEP)
    ap.add_argument("--timeout", type=float, default=3.0)
    ap.add_argument("--concurrency", type=int, default=300)
    ap.add_argument("--no-screen", action="store_true")
    args = ap.parse_args()
    print(f"[1/2] 采集（{len(SOURCES)} 个源）")
    stats = build(args.out, args.limit, args.timeout,
                  args.concurrency, not args.no_screen)
    print(f"[2/2] 完成：kept={stats['kept']}")
    if stats["kept"] < MIN_OK:
        print(f"警告：kept < {MIN_OK}，本轮结果异常", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
