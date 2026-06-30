#!/usr/bin/env python3
"""
台股 KD 低檔站上 5MA 放量篩選 — GitHub Pages updater.

Conditions:
- 9-day KD: K < 30 and D < 30
- Close crosses upward above 5-day moving average
- Today's volume > 2x previous 5 trading days' average volume

Outputs:
- tw-stocks/kd-ma5-volume/data.json
- tw-stocks/kd-ma5-volume/index.html
- optional GitHub Contents API push when GITHUB_TOKEN/GH_TOKEN/gh token exists
"""
from __future__ import annotations

import base64
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

HEAD = {"User-Agent": "Mozilla/5.0 (Hermes Vivy Taiwan stock screener)"}
REPO = "RaisoLiu/ai-tools-sharing-talk"
REMOTE_BASE = "tw-stocks/kd-ma5-volume"
ROOT = Path(__file__).resolve().parents[2]
WEB_DIR = ROOT / "tw-stocks" / "kd-ma5-volume"
TZ = ZoneInfo("Asia/Taipei")


def get_json(url: str, retries: int = 3):
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEAD)
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read().decode("utf-8-sig"))
        except urllib.error.HTTPError as e:
            last = e
            if e.code in (403, 429, 503):
                time.sleep(1.5 * (i + 1))
                continue
            raise
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(1.2 * (i + 1))
    raise last  # type: ignore[misc]


def is_common_stock(code: str) -> bool:
    return bool(re.fullmatch(r"\d{4}", code)) and not code.startswith("0")


def load_universe() -> dict[str, dict[str, str]]:
    codes: dict[str, dict[str, str]] = {}
    for x in get_json("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"):
        c = str(x.get("Code", "")).strip()
        if is_common_stock(c):
            codes[c] = {"name": x.get("Name", ""), "market": "上市", "suffix": ".TW"}
    try:
        for x in get_json("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"):
            c = str(x.get("SecuritiesCompanyCode", "")).strip()
            if is_common_stock(c):
                codes[c] = {"name": x.get("CompanyName", ""), "market": "上櫃", "suffix": ".TWO"}
    except Exception as e:  # noqa: BLE001
        print(f"⚠️ TPEX universe failed: {e}", file=sys.stderr)
    return codes


def fetch_yahoo(code: str, suffix: str) -> list[dict[str, float | str]]:
    sym = f"{code}{suffix}"
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?range=3mo&interval=1d"
    js = get_json(url, retries=2)
    result = (js.get("chart", {}).get("result") or [None])[0]
    if not result:
        raise RuntimeError(js.get("chart", {}).get("error") or "no chart result")
    ts = result.get("timestamp") or []
    q = result.get("indicators", {}).get("quote", [{}])[0]
    rows = []
    for t, op, hi, lo, cl, vol in zip(
        ts,
        q.get("open", []),
        q.get("high", []),
        q.get("low", []),
        q.get("close", []),
        q.get("volume", []),
        strict=False,
    ):
        if None in (hi, lo, cl, vol):
            continue
        date = datetime.fromtimestamp(t, TZ).strftime("%Y-%m-%d")
        rows.append({"date": date, "open": op, "high": hi, "low": lo, "close": cl, "volume": float(vol)})
    return rows


def calc_kd(rows: list[dict[str, float | str]]) -> list[tuple[float | None, float | None]]:
    k = d = 50.0
    out: list[tuple[float | None, float | None]] = []
    for i, row in enumerate(rows):
        if i < 8:
            out.append((None, None))
            continue
        win = rows[i - 8 : i + 1]
        hh = max(float(x["high"]) for x in win)
        ll = min(float(x["low"]) for x in win)
        rsv = 50.0 if hh == ll else (float(row["close"]) - ll) / (hh - ll) * 100
        k = 2 * k / 3 + rsv / 3
        d = 2 * d / 3 + k / 3
        out.append((k, d))
    return out


def screen_one(code: str, meta: dict[str, str]):
    rows = fetch_yahoo(code, meta["suffix"])
    if len(rows) < 14:
        return None
    i, p = len(rows) - 1, len(rows) - 2
    kd = calc_kd(rows)
    k, d = kd[i]
    closes = [float(r["close"]) for r in rows]
    vols = [float(r["volume"]) for r in rows]
    ma5 = [None] * len(rows)
    for j in range(4, len(rows)):
        ma5[j] = sum(closes[j - 4 : j + 1]) / 5
    if None in (k, d, ma5[i], ma5[p]):
        return None
    vol_avg_prev5 = sum(vols[i - 5 : i]) / 5
    if k < 30 and d < 30 and closes[p] <= ma5[p] and closes[i] > ma5[i] and vols[i] > 2 * vol_avg_prev5:
        return {
            "code": code,
            "name": meta["name"],
            "market": meta["market"],
            "date": rows[i]["date"],
            "close": round(closes[i], 2),
            "change_pct": round((closes[i] / closes[p] - 1) * 100, 2),
            "K": round(float(k), 1),
            "D": round(float(d), 1),
            "ma5": round(float(ma5[i]), 2),
            "prev_close": round(closes[p], 2),
            "prev_ma5": round(float(ma5[p]), 2),
            "volume": int(round(vols[i])),
            "vol_avg5_prev": int(round(vol_avg_prev5)),
            "vol_ratio": round(vols[i] / vol_avg_prev5, 2),
            "ma5_turn_up": bool(ma5[i] > ma5[p]),
        }
    return None


def fmt_int(x: float | int) -> str:
    return f"{int(round(x)):,}"


def render_index() -> str:
    return """<!doctype html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>台股 KD 低檔站上 5MA 放量篩選</title>
<style>
:root{color-scheme:dark;--bg:#07111f;--card:#101b2d;--muted:#91a4bd;--text:#eef5ff;--line:#22324b;--good:#6ee7b7;--accent:#7dd3fc;--warn:#fbbf24;--bad:#fb7185}*{box-sizing:border-box}body{margin:0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans TC',sans-serif;background:radial-gradient(circle at top left,#17365f 0,#07111f 42%,#050812 100%);color:var(--text)}main{max-width:1180px;margin:0 auto;padding:36px 16px 52px}.hero{display:flex;justify-content:space-between;gap:20px;align-items:flex-end;margin-bottom:20px}h1{margin:0;font-size:clamp(28px,5vw,52px);letter-spacing:-.04em}.subtitle{color:var(--muted);margin-top:10px;line-height:1.7}.badge{border:1px solid #315075;background:rgba(125,211,252,.09);padding:10px 14px;border-radius:999px;color:var(--accent);white-space:nowrap}.cards{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin:20px 0}.card{background:rgba(16,27,45,.84);border:1px solid var(--line);border-radius:18px;padding:16px;box-shadow:0 20px 60px rgba(0,0,0,.25)}.k{color:var(--muted);font-size:13px}.v{font-size:24px;font-weight:750;margin-top:6px}.table-wrap{overflow:auto;background:rgba(16,27,45,.88);border:1px solid var(--line);border-radius:22px}table{width:100%;border-collapse:collapse;min-width:920px}th,td{padding:13px 14px;border-bottom:1px solid var(--line);text-align:left;vertical-align:middle}th{color:#b8c7dc;font-size:13px;background:rgba(255,255,255,.03);position:sticky;top:0}td{font-size:14px}.num{text-align:right;font-variant-numeric:tabular-nums}.good{color:var(--good)}.bad{color:var(--bad)}.empty{padding:36px;text-align:center;color:var(--muted);background:rgba(16,27,45,.88);border:1px solid var(--line);border-radius:22px}.notes{margin-top:18px;color:var(--muted);line-height:1.75;font-size:14px}.pill{display:inline-block;color:var(--warn);border:1px solid rgba(251,191,36,.35);border-radius:999px;padding:3px 9px;margin-right:8px}@media(max-width:760px){.hero{display:block}.badge{display:inline-block;margin-top:14px}.cards{grid-template-columns:1fr 1fr}}
</style>
</head>
<body><main>
<section class="hero"><div><h1>台股 KD 低檔站上 5MA 放量篩選</h1><div class="subtitle" id="subtitle">載入中...</div></div><div class="badge">每日自動更新・非投資建議</div></section>
<section class="cards"><div class="card"><div class="k">符合條件</div><div class="v" id="count">—</div></div><div class="card"><div class="k">資料日</div><div class="v" id="date">—</div></div><div class="card"><div class="k">最高量比</div><div class="v" id="maxVol">—</div></div><div class="card"><div class="k">最低 K/D</div><div class="v" id="minKD">—</div></div></section>
<div id="content" class="empty">載入中...</div>
<section class="notes"><p><span class="pill">規則</span>K&lt;30、D&lt;30；前一交易日收盤 ≤ 5MA，當日收盤 &gt; 5MA；當日成交量 &gt; 前 5 日均量 2 倍。</p><p><span class="pill">來源</span>TWSE/TPEX OpenAPI 取得股票 universe；Yahoo chart API 取得近 3 個月 OHLCV。</p><p><span class="pill">提醒</span>這只是條件式篩選，不是買賣建議；隔日請複核是否守住 5MA、量能是否延續。</p></section>
</main><script>
const fmtInt=n=>Math.round(n).toLocaleString('en-US');
function render(d){
  document.getElementById('subtitle').innerHTML=`更新時間：${d.updated || '—'}<br>資料日：${d.latest_date || '—'}｜掃描 ${d.universe_count || 0} 檔｜抓取錯誤 ${d.error_count || 0} 檔`;
  document.getElementById('count').textContent=(d.matches||[]).length+' 檔';
  document.getElementById('date').textContent=d.latest_date || '—';
  const ms=d.matches||[];
  document.getElementById('maxVol').textContent=ms.length?Math.max(...ms.map(x=>x.vol_ratio)).toFixed(2)+'x':'—';
  document.getElementById('minKD').textContent=ms.length?ms.reduce((a,b)=>(a.K+a.D)<(b.K+b.D)?a:b).K+'/'+ms.reduce((a,b)=>(a.K+a.D)<(b.K+b.D)?a:b).D:'—';
  if(!ms.length){document.getElementById('content').className='empty';document.getElementById('content').textContent='今天沒有符合條件的標的。';return;}
  document.getElementById('content').className='table-wrap';
  document.getElementById('content').innerHTML=`<table><thead><tr><th>代號</th><th>名稱</th><th>市場</th><th class="num">收盤</th><th class="num">漲跌幅</th><th class="num">K/D</th><th class="num">5MA</th><th class="num">成交量</th><th class="num">前5日均量</th><th class="num">量比</th></tr></thead><tbody>${ms.map(s=>`<tr><td><strong>${s.code}</strong></td><td>${s.name}</td><td>${s.market}</td><td class="num">${Number(s.close).toFixed(2)}</td><td class="num ${s.change_pct>=0?'good':'bad'}">${s.change_pct>=0?'+':''}${s.change_pct.toFixed(2)}%</td><td class="num good">${s.K}/${s.D}</td><td class="num">${Number(s.ma5).toFixed(2)}</td><td class="num">${fmtInt(s.volume)}</td><td class="num">${fmtInt(s.vol_avg5_prev)}</td><td class="num good">${s.vol_ratio.toFixed(2)}x</td></tr>`).join('')}</tbody></table>`;
}
fetch('data.json?'+Date.now()).then(r=>r.json()).then(render).catch(e=>{document.getElementById('content').className='empty';document.getElementById('content').textContent='載入失敗：'+e;});
</script></body></html>
"""


def write_outputs(payload: dict) -> None:
    WEB_DIR.mkdir(parents=True, exist_ok=True)
    (WEB_DIR / "data.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (WEB_DIR / "index.html").write_text(render_index(), encoding="utf-8")


def get_github_token() -> str | None:
    for key in ("GITHUB_TOKEN", "GH_TOKEN"):
        if os.environ.get(key):
            return os.environ[key]
    try:
        token = subprocess.check_output(["gh", "auth", "token"], text=True, timeout=10).strip()
        if token:
            return token
    except Exception:
        pass
    cred_paths = [Path.home() / ".git-credentials", Path("/tmp/git-creds.txt")]
    for p in cred_paths:
        if p.exists():
            text = p.read_text(errors="ignore")
            m = re.search(r":((?:gho|ghp|github_pat)_[^@\s]+)@", text)
            if m:
                return m.group(1)
    return None


def gh_push_file(remote_path: str, local_path: Path, message: str, token: str) -> bool:
    api_url = f"https://api.github.com/repos/{REPO}/contents/{remote_path}"
    sha = ""
    try:
        req = urllib.request.Request(api_url, headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"})
        with urllib.request.urlopen(req, timeout=15) as r:
            sha = json.loads(r.read().decode()).get("sha", "")
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
    content_b64 = base64.b64encode(local_path.read_bytes()).decode()
    payload = {"message": message, "content": content_b64}
    if sha:
        payload["sha"] = sha
    put_req = urllib.request.Request(
        api_url,
        data=json.dumps(payload).encode(),
        method="PUT",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(put_req, timeout=30) as r:
        result = json.loads(r.read().decode())
    return "content" in result and "commit" in result


def push_outputs() -> bool:
    token = get_github_token()
    if not token:
        print("⚠️ 找不到 GitHub token，僅更新本機檔案", file=sys.stderr)
        return False
    date_str = datetime.now(TZ).strftime("%Y-%m-%d")
    ok1 = gh_push_file(f"{REMOTE_BASE}/data.json", WEB_DIR / "data.json", f"auto: update KD MA5 volume screener data ({date_str})", token)
    ok2 = gh_push_file(f"{REMOTE_BASE}/index.html", WEB_DIR / "index.html", f"auto: update KD MA5 volume screener page ({date_str})", token)
    return ok1 and ok2


def main() -> str:
    codes = load_universe()
    matches, errors = [], []
    with ThreadPoolExecutor(max_workers=24) as ex:
        futures = {ex.submit(screen_one, c, m): (c, m) for c, m in codes.items()}
        for fut in as_completed(futures):
            c, _m = futures[fut]
            try:
                r = fut.result()
                if r:
                    matches.append(r)
            except Exception as e:  # noqa: BLE001
                errors.append((c, str(e)[:100]))
    matches.sort(key=lambda x: (-x["vol_ratio"], x["code"]))
    latest = max((x["date"] for x in matches), default="—")
    payload = {
        "title": "台股 KD 低檔站上 5MA 放量篩選",
        "updated": datetime.now(TZ).strftime("%Y-%m-%d %H:%M Asia/Taipei"),
        "latest_date": latest,
        "rules": "K<30、D<30；收盤由下往上站上5MA；成交量 > 前5日均量2倍",
        "sources": ["TWSE OpenAPI", "TPEX OpenAPI", "Yahoo chart API"],
        "universe_count": len(codes),
        "error_count": len(errors),
        "matches": matches,
    }
    write_outputs(payload)

    lines = [
        "📈 台股 KD 低檔站上 5MA 放量篩選",
        f"更新時間：{payload['updated']}｜資料日：{latest}",
        f"掃描：{len(codes)} 檔｜符合：{len(matches)} 檔｜抓取錯誤：{len(errors)} 檔",
        "網頁：<https://raisoliu.github.io/ai-tools-sharing-talk/tw-stocks/kd-ma5-volume/>",
    ]
    if matches:
        lines.append("")
        for x in matches[:20]:
            lines.append(
                f"{x['code']} {x['name']}（{x['market']}）｜收 {x['close']:.2f}（{x['change_pct']:+.2f}%）｜"
                f"K/D {x['K']:.1f}/{x['D']:.1f}｜5MA {x['ma5']:.2f}｜"
                f"量 {fmt_int(x['volume'])} / 均 {fmt_int(x['vol_avg5_prev'])}（{x['vol_ratio']:.2f}x）"
            )
        if len(matches) > 20:
            lines.append(f"…另有 {len(matches) - 20} 檔請看網頁。")
    else:
        lines.append("今天沒有符合條件的標的。")
    if len(errors) > max(150, len(codes) * 0.35):
        lines.append(f"\n⚠️ 資料抓取錯誤偏多（{len(errors)}/{len(codes)}），可能是不完整掃描。")
    lines.append("\n提醒：這是條件式篩選，不是投資建議；下單前請用券商/交易所資料複核。")
    return "\n".join(lines)


if __name__ == "__main__":
    output = main()
    print(output)
    (Path("/tmp") / "tw_stock_kd_ma5_volume_result.txt").write_text(output, encoding="utf-8")
    try:
        if push_outputs():
            print("✅ GitHub Pages push 成功", file=sys.stderr)
        else:
            print("⚠️ GitHub Pages push 未完成", file=sys.stderr)
    except Exception as e:  # noqa: BLE001
        print(f"⚠️ GitHub push 例外: {e}", file=sys.stderr)
