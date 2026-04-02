#!/usr/bin/env python3
"""
台股篩選 v3 — 每日掃描腳本
規則：5EMA>20EMA>60EMA ≥5日 + 起漲週均>2% + 離EMA20<20% + 均額>2000M
投信%：顯示 起漲日→現在 的變化
"""
import json, urllib.request, subprocess, re, sys, time, os
import warnings
warnings.filterwarnings('ignore')

def fetch_twse_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read().decode('utf-8'))

def fetch_t86(date_str):
    """Fetch 三大法人買賣超日報 for a given date (YYYYMMDD). Returns {code: 投信買賣超股數}"""
    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date={date_str}&selectType=ALLBUT0999"
    data = fetch_twse_json(url)
    result = {}
    for row in data.get('data', []):
        code = row[0].strip()
        try:
            # 投信買賣超股數 = index 10
            net = int(row[10].replace(',', ''))
            result[code] = net
        except:
            pass
    return result

def main():
    import yfinance as yf
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta

    print("⏳ 下載市場資料...", file=sys.stderr)

    # 1. Get stock names from TWSE
    mkt = fetch_twse_json("https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json&type=ALLBUT0999")
    namemap = {}
    codes = []
    for t in mkt.get('tables', []):
        if '每日收盤行情' in t.get('title', ''):
            for r in t['data']:
                c = r[0].strip()
                namemap[c] = r[1].strip()
                if len(c) == 4 and c.isdigit():
                    codes.append(c)
            break
    trade_date = mkt.get('date', '?')

    # 2. Get 發行股數（以市場當日為基準，若當日無資料往前找）
    shares_map = {}
    from datetime import datetime as _dt2
    try:
        ref = _dt2.strptime(str(trade_date), '%Y%m%d')
    except:
        ref = _dt2.now()
    for delta in range(0, 7):
        try:
            d = (ref - timedelta(days=delta)).strftime('%Y%m%d')
            shares_data = fetch_twse_json(f"https://www.twse.com.tw/rwd/zh/fund/MI_QFIIS?date={d}&selectType=ALLBUT0999&response=json")
            if shares_data.get('data'):
                for row in shares_data['data']:
                    shares_map[row[0].strip()] = int(row[3].replace(',', ''))
                print(f"✅ 發行股數資料來源: {d} ({len(shares_map)} 檔)", file=sys.stderr)
                break
        except:
            continue

    # 3. Download all price data (6 months for EMA60)
    print("⏳ 下載價格資料 (yfinance)...", file=sys.stderr)
    tickers = " ".join(f"{c}.TW" for c in codes)
    data = yf.download(tickers, period="6mo", interval="1d", progress=False, threads=True)

    # 4. Screen stocks
    print("⏳ 篩選中...", file=sys.stderr)
    results = []
    for code in codes:
        ticker = f"{code}.TW"
        try:
            if isinstance(data.columns, pd.MultiIndex):
                df = data.xs(ticker, level=1, axis=1).dropna(subset=['Close'])
            else:
                continue
            if len(df) < 65:
                continue

            df['Turnover'] = df['Close'] * df['Volume']
            if float(df['Turnover'].tail(30).mean()) < 2e9:
                continue

            df['EMA5'] = df['Close'].ewm(span=5, adjust=False).mean()
            df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()
            df['EMA60'] = df['Close'].ewm(span=60, adjust=False).mean()

            aligned = (df['EMA5'] > df['EMA20']) & (df['EMA20'] > df['EMA60'])
            if not aligned.iloc[-5:].all():
                continue

            streak = 0
            for v in reversed(aligned.values):
                if v: streak += 1
                else: break

            close = float(df['Close'].iloc[-1])
            e20 = float(df['EMA20'].iloc[-1])
            dist20 = (close / e20 - 1) * 100

            start_idx = len(df) - streak
            if start_idx < 0:
                start_idx = 0

            # 週均 = 日均漲幅 × 5（交易日）
            streak_df = df.iloc[start_idx:]
            daily_returns = streak_df['Close'].pct_change().dropna()
            if len(daily_returns) < 1:
                continue
            avg_wk = float(daily_returns.mean() * 5 * 100)
            if avg_wk <= 2.0:
                continue

            start_price = float(df['Close'].iloc[max(start_idx - 1, 0)])
            start_date_dt = df.index[start_idx]
            start_date = start_date_dt.strftime('%m/%d')
            rise_pct = (close / start_price - 1) * 100
            turnover_m = float(df['Turnover'].tail(30).mean()) / 1e6

            # Collect trading dates from start to end for T86 lookup
            trading_dates = [d.strftime('%Y%m%d') for d in df.index[start_idx:]]

            mktcap_b = round(close * shares_map.get(code, 0) / 1e9, 0) if shares_map.get(code) else 0
            if mktcap_b > 0 and mktcap_b < 100:
                continue

            results.append({
                'code': code,
                'name': namemap.get(code, '?'),
                'close': close,
                'streak': streak,
                'start_date': start_date,
                'start_date_dt': start_date_dt,
                'rise_pct': round(rise_pct, 1),
                'avg_wk': round(avg_wk, 2),
                'turnover_m': round(turnover_m, 0),
                'trading_dates': trading_dates,
                'dist20': round(dist20, 1),
                'mktcap_b': mktcap_b,
            })
        except:
            continue

    all_stocks = results  # for T86/trust lookups

    # 5. 投信持股% — 從本地 T86 cache + fubon 計算起漲→現在
    T86_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 't86')
    target_codes = set(r['code'] for r in all_stocks)

    # 5a. Load T86 from local cache, fetch missing dates
    all_dates = set()
    for r in all_stocks:
        all_dates.update(r['trading_dates'])
    all_dates = sorted(all_dates)

    t86_cache = {}
    missing_dates = []
    for d in all_dates:
        cache_path = os.path.join(T86_DIR, f'{d}.json')
        if os.path.exists(cache_path):
            with open(cache_path) as f:
                t86_cache[d] = json.load(f)
        else:
            missing_dates.append(d)

    if missing_dates:
        os.makedirs(T86_DIR, exist_ok=True)
        print(f"⏳ 抓取 {len(missing_dates)} 個缺少的 T86 交易日...", file=sys.stderr)
        for i, d in enumerate(missing_dates):
            try:
                data = fetch_t86(d)
                t86_cache[d] = data
                if data:
                    with open(os.path.join(T86_DIR, f'{d}.json'), 'w') as f:
                        json.dump(data, f)
                if (i + 1) % 10 == 0:
                    print(f"  ... {i+1}/{len(missing_dates)}", file=sys.stderr)
            except:
                t86_cache[d] = {}
            time.sleep(3.5)

    # 5b. Get current 投信持股(張) from fubon
    print("⏳ 抓取現在投信持股%...", file=sys.stderr)
    current_trust_k = {}  # code -> 投信持股(千股/張)
    for code in target_codes:
        try:
            url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcl/zcl_{code}.djhtm"
            cmd = f'timeout 8 curl -s "{url}" -H "User-Agent: Mozilla/5.0"'
            r = subprocess.run(cmd, shell=True, capture_output=True, timeout=12)
            text = r.stdout.decode('big5', errors='replace')
            # Parse rows from HTML table
            rows = re.findall(r'<tr[^>]*>(.*?)</tr>', text, re.DOTALL)
            for row in rows:
                cells_raw = re.findall(r'<td[^>]*class="t3[nt]\d?"[^>]*>([^<]*)', row)
                cells = [c.strip() for c in cells_raw if c.strip()]
                if cells and re.match(r'1\d{2}/\d{2}/\d{2}', cells[0]):
                    # Layout varies (9 or 11 cols) but 投信持股 is always cells[-5]
                    if len(cells) >= 7:
                        try:
                            current_trust_k[code] = int(cells[-5].replace(',', ''))
                        except:
                            pass
                    break
        except:
            pass

    # 5c. Calculate 起漲日投信% and 現在投信%
    trust_start_pct = {}
    trust_now_pct = {}
    for r in all_stocks:
        code = r['code']
        total = shares_map.get(code, 0)
        if code not in current_trust_k or total == 0:
            continue
        now_shares = current_trust_k[code] * 1000  # 張→股
        now_pct = round(now_shares / total * 100, 1)
        # Cumulative T86 net buys during streak
        cum_net = 0
        for d in r['trading_dates']:
            cum_net += t86_cache.get(d, {}).get(code, 0)
        start_shares = now_shares - cum_net
        start_pct = round(max(start_shares, 0) / total * 100, 1)
        trust_now_pct[code] = now_pct
        trust_start_pct[code] = start_pct

    # 6. Sort by streak ascending
    results.sort(key=lambda x: x['streak'])

    # 7. Split into 3 groups by 起漲日
    from datetime import datetime as _dt
    # Parse trade_date (YYYYMMDD) to get reference date
    try:
        ref_date = _dt.strptime(str(trade_date), '%Y%m%d')
    except:
        ref_date = _dt.now()
    one_month_ago = ref_date - timedelta(days=30)
    three_months_ago = ref_date - timedelta(days=90)

    t1, t2, t3 = [], [], []
    for r in results:
        dt = r['start_date_dt']
        if hasattr(dt, 'to_pydatetime'):
            dt = dt.to_pydatetime()
        if hasattr(dt, 'tzinfo') and dt.tzinfo:
            dt = dt.replace(tzinfo=None)
        if dt >= one_month_ago:
            t1.append(r)
        elif dt >= three_months_ago:
            t2.append(r)
        else:
            t3.append(r)

    # 8. Format output
    def fmt_table(stock_list):
        tbl = []
        tbl.append(f"{'代號':<6}{'名稱':<12}{'市值':>6}{'起漲':>5}{'持續':>4}{'漲幅':>8}{'週均':>8}{'乖離':>7}{'均額':>8} {'投信%':>14}")
        for r in stock_list:
            code = r['code']
            if code in trust_start_pct:
                tp_str = f"{trust_start_pct[code]}→{trust_now_pct[code]}%"
            else:
                tp_str = "—"
            mc = r.get('mktcap_b', 0)
            mc_str = f"{mc:.0f}B" if mc else "—"
            tbl.append(
                f"{code:<6}{r['name']:<12}{mc_str:>6}{r['start_date']:>5}"
                f"{r['streak']:>4}"
                f"{r['rise_pct']:>+7.1f}%"
                f"{r['avg_wk']:>+7.2f}%"
                f"{r['dist20']:>+6.1f}%"
                f"{r['turnover_m']:>7,.0f}M"
                f" {tp_str:>14}"
            )
        return "\n".join(tbl)

    lines = []
    lines.append(f"📋 **台股篩選 v3 — {trade_date}**")
    lines.append(f"規則：5EMA>20EMA>60EMA ≥5日 + 起漲週均>2% + 均額>2000M + 市值>100B")
    lines.append(f"共 {len(results)} 檔")
    lines.append("")

    if t1:
        lines.append(f"🟢 **剛起漲 (<1個月)** — {len(t1)} 檔")
        lines.append("```")
        lines.append(fmt_table(t1))
        lines.append("```")

    if t2:
        lines.append(f"🟡 **中段加速 (1~3個月)** — {len(t2)} 檔")
        lines.append("```")
        lines.append(fmt_table(t2))
        lines.append("```")

    if t3:
        lines.append(f"🔴 **長線主升 (>3個月)** — {len(t3)} 檔")
        lines.append("```")
        lines.append(fmt_table(t3))
        lines.append("```")

    # 9. Export JSON for web
    web_dir = os.path.join(os.path.dirname(__file__), '..', 'web')
    os.makedirs(web_dir, exist_ok=True)
    web_data = {
        'date': str(trade_date),
        'updated': _dt.now().strftime('%Y-%m-%d %H:%M'),
        'rules': '5EMA>20EMA>60EMA ≥5日 + 起漲週均>2% + 均額>2000M + 市值>100B',
        'stocks': []
    }
    for r in results:
        code = r['code']
        dt = r['start_date_dt']
        if hasattr(dt, 'to_pydatetime'):
            dt = dt.to_pydatetime()
        if hasattr(dt, 'tzinfo') and dt.tzinfo:
            dt = dt.replace(tzinfo=None)
        tier = 1 if dt >= one_month_ago else (2 if dt >= three_months_ago else 3)
        web_data['stocks'].append({
            'code': code,
            'name': r['name'],
            'mktcap_b': r.get('mktcap_b', 0),
            'start_date': r['start_date'],
            'streak': r['streak'],
            'rise_pct': r['rise_pct'],
            'avg_wk': r['avg_wk'],
            'dist20': r['dist20'],
            'turnover_m': r['turnover_m'],
            'trust_start': trust_start_pct.get(code),
            'trust_now': trust_now_pct.get(code),
            'tier': tier,
        })
    with open(os.path.join(web_dir, 'data.json'), 'w') as f:
        json.dump(web_data, f, ensure_ascii=False)

    return "\n".join(lines)


if __name__ == "__main__":
    output = main()
    print(output)
    with open("/tmp/tw_stock_scan_result.txt", "w") as f:
        f.write(output)

    # Push updated data.json to GitHub via API (git push hangs due to network issues)
    try:
        import base64, urllib.error
        creds_file = '/tmp/git-creds.txt'
        if os.path.exists(creds_file):
            with open(creds_file) as f:
                creds = f.read().strip()
            import re as _re
            m = _re.search(r':(gho_[^@]+)@', creds)
            token = m.group(1) if m else None
        else:
            token = None

        if token:
            repo = 'RaisoLiu/ai-tools-sharing-talk'
            web_dir_local = os.path.join(os.path.dirname(__file__), '..', 'web')
            date_str = _dt.now().strftime('%Y-%m-%d')

            def gh_push(remote_path, local_path, msg):
                api_url = f'https://api.github.com/repos/{repo}/contents/{remote_path}'
                try:
                    req = urllib.request.Request(api_url, headers={'Authorization': f'token {token}'})
                    resp = json.loads(urllib.request.urlopen(req, timeout=10).read())
                    sha = resp.get('sha', '')
                except:
                    sha = ''
                with open(local_path, 'rb') as f:
                    content_b64 = base64.b64encode(f.read()).decode()
                payload = json.dumps({'message': msg, 'content': content_b64, 'sha': sha} if sha else {'message': msg, 'content': content_b64}).encode()
                put_req = urllib.request.Request(api_url, data=payload, method='PUT',
                    headers={'Authorization': f'token {token}', 'Content-Type': 'application/json'})
                result = json.loads(urllib.request.urlopen(put_req, timeout=30).read())
                return 'content' in result

            ok1 = gh_push('tw-stocks/web/data.json', os.path.join(web_dir_local, 'data.json'),
                          f'auto: update tw-stocks data.json ({date_str})')
            ok2 = gh_push('tw-stocks/web/index.html', os.path.join(web_dir_local, 'index.html'),
                          f'auto: update tw-stocks index.html ({date_str})')
            if ok1 and ok2:
                print("✅ GitHub Pages push 成功", file=sys.stderr)
            else:
                print(f"⚠️ GitHub Pages push 部分失敗 data={ok1} html={ok2}", file=sys.stderr)
        else:
            print("⚠️ 找不到 GitHub token，跳過 push", file=sys.stderr)
    except Exception as e:
        print(f"⚠️ GitHub push 例外: {e}", file=sys.stderr)
