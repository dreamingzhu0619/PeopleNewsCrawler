import nest_asyncio, asyncio
nest_asyncio.apply()   # 打补丁，允许嵌套事件循环
import os
import time
import json
import re
import requests
from datetime import datetime, timedelta
import pandas as pd
from bs4 import BeautifulSoup
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
import chardet

# ========== 文件路径 ==========
DESKTOP = os.path.expanduser("~/Desktop")
OUT_XLSX = os.path.join(DESKTOP, "people_news_final_deduped.xlsx")
TEMP_CSV = os.path.join(DESKTOP, "people_news_backup_final_deduped.csv")

# ========== 时间范围 ==========
START_DATE = datetime(2024, 4, 1)
END_DATE = datetime(2024, 5, 1)
WINDOW = 1  # 天

# ========== 搜索关键词 ==========
SEARCH_KEY = "政策"

# ========== API 配置 ==========
API_URL = "http://search.people.cn/search-platform/front/search"
HEADERS = {
    "Content-Type": "application/json;charset=UTF-8",
    "User-Agent": "Mozilla/5.0",
    # 【重要】请务必更新您的Cookie
    "Cookie": "__jsluid_h=701a2860ad98e437adf837c0f2588434; sso_c=0; sfr=1"
}

BASE_PAYLOAD = {
    "key": SEARCH_KEY, "page": 1, "limit": 10, "hasTitle": True,
    "hasContent": True, "isFuzzy": True, "type": 1, "sortType": 2,
}

# ========== 关键词 ==========
KEYWORD_SET = {
    "经济", "金融", "商业", "不确定", "不明确", "未明", "不明朗", "不清晰", "未清晰",
    "难料", "难以预料", "难以预测", "难以预计", "难以估计", "无法预料", "无法预测",
    "无法预计", "无法估计", "不可预料", "不可预测", "不可预计", "不可估计", "波动",
    "震荡", "动荡", "不稳", "未知", "政策", "制度", "体制", "战略", "措施", "规章",
    "规例", "条例", "政治", "执政", "政府", "国务院", "人大", "人民代表大会",
    "中央", "国家领导人", "总理", "改革", "整改", "整治", "规管", "监管", "财政",
    "税", "人民银行", "央行", "赤字", "利率"
}
# 使用模糊匹配以捕获更多相关文章
KEYWORD_PATTERN = re.compile('|'.join(KEYWORD_SET))


# ========== 核心函数 ==========

def get_uniqueness_key(row):
    """【新增】根据标题和日期生成文章的唯一“指纹”"""
    title = row.get('标题', '')
    # 清理标题，移除所有非字母数字的字符，抵抗排版差异
    cleaned_title = re.sub(r'\W+', '', title)
    # 取标题前50个字符作为指纹，避免长标题末尾的微小差异
    title_fingerprint = cleaned_title[:50]
    date_str = row.get('时间', '').split(' ')[0]  # 确保只取日期
    return f"{date_str}_{title_fingerprint}"


def text_hits(text):
    text = text.replace(" ", "")
    hits = set(KEYWORD_PATTERN.findall(text))
    return list(hits)


def clean(x):
    return re.sub(r"\s+", " ", str(x or "")).strip()


def parse_time(x):
    if not x: return None
    try:
        if isinstance(x, (int, float)):
            return datetime.fromtimestamp(int(x / 1000) if x > 1e11 else int(x))
        return datetime.strptime(x[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


async def fetch_html_async(session, url, retries=3):
    headers = {"User-Agent": "Mozilla/5.0"}
    for attempt in range(retries):
        try:
            async with session.get(url, headers=headers, timeout=15) as response:
                if response.status == 200:
                    content = await response.read()
                    encoding = chardet.detect(content)['encoding'] or 'utf-8'
                    return content.decode(encoding, errors='ignore')
                elif response.status in [429, 500, 502, 503, 504]:
                    await asyncio.sleep(2 ** attempt)
                else:
                    return ""
        except asyncio.TimeoutError:
            await asyncio.sleep(2 ** attempt)
        except Exception:
            return ""
    print(f"    ❌ 多次重试失败: {url}")
    return ""


def extract_body(html):
    if not html: return ""
    soup = BeautifulSoup(html, "lxml")
    selectors = ["#rwb_zw", ".rm_txt_con", ".rm_txt", ".article-content", ".article",
                 ".content", ".main-content", "#articleContent", ".text_content"]
    for sel in selectors:
        node = soup.select_one(sel)
        if node:
            for tag in node.select("script,style,.zdfy,.editor,.related-news,a,span"):
                tag.decompose()
            body = clean(node.get_text(" "))
            if len(body) > 50: return body
    paragraphs = soup.find_all('p')
    if paragraphs:
        body = clean(' '.join([p.get_text(" ") for p in paragraphs]))
        if len(body) > 50: return body
    return ""


async def process_news_batch(news_items):
    async with aiohttp.ClientSession() as session:
        tasks, valid_items = [], []
        for item in news_items:
            url = clean(item.get("url") or item.get("originUrl"))
            if not url or not url.startswith("http") or "video" in url: continue
            valid_items.append(item)
            tasks.append(fetch_html_async(session, url))

        if not tasks: return []
        print(f"  🚀 并发请求 {len(tasks)} 个URL")
        htmls = await asyncio.gather(*tasks)

        with ThreadPoolExecutor(max_workers=10) as executor:
            loop = asyncio.get_event_loop()
            extract_tasks = [loop.run_in_executor(executor, extract_body, html) for html in htmls]
            bodies = await asyncio.gather(*extract_tasks)

        results = []
        for item, body in zip(valid_items, bodies):
            title = clean(item.get("title") or "无标题")
            if not body or len(body) < 100:
                print(f"    📄 正文过短或为空，跳过: {title}")
                continue
            hits = text_hits(body)
            if not hits:
                print(f"    🔍 未命中关键词，跳过: {title}")
                continue

            pub = parse_time(item.get("displayTime") or item.get("publishTime"))
            results.append({
                "标题": title,
                "时间": pub.strftime("%Y-%m-%d") if pub else "",
                "URL": clean(item.get("url")),
                "正文": body, "字数": len(body), "命中关键词": ",".join(hits)
            })
        return results


# ========== 主逻辑 (带智能去重) ==========
def main():
    # 【优化】使用“唯一指纹”为键的字典，实现内容去重
    all_data = {}
    if os.path.exists(TEMP_CSV):
        try:
            df_backup = pd.read_csv(TEMP_CSV)
            for row in df_backup.to_dict("records"):
                key = get_uniqueness_key(row)
                all_data[key] = row
            print(f"✅ 缓存恢复 {len(all_data)} 条不重复记录")
        except Exception as e:
            print(f"⚠️ 无法读取缓存文件: {e}")

    session = requests.Session();
    session.headers.update(HEADERS)
    current, save_counter = END_DATE, 0
    save_interval = 20

    while current > START_DATE:
        prev = current - timedelta(days=WINDOW)
        print(f"\n📅 抓取时间段: {prev.date()} → {current.date()}")
        page = 1
        while True:
            payload = BASE_PAYLOAD.copy()
            payload.update(
                {"page": page, "startTime": int(prev.timestamp() * 1000), "endTime": int(current.timestamp() * 1000)})
            try:
                print(f"  📡 请求API第 {page} 页")
                resp = session.post(API_URL, json=payload, timeout=12)
                resp.raise_for_status()
                recs = resp.json().get("data", {}).get("records") or []
            except Exception as e:
                print(f"⚠️ API请求失败: {e}，休息5秒");
                time.sleep(5);
                continue

            if not recs: print("  📄 第 {page} 页没有更多记录"); break
            print(f"  📥 第 {page} 页获取到 {len(recs)} 条记录")

            processed_rows = asyncio.run(process_news_batch(recs))
            newly_added_count = 0
            if processed_rows:
                for row in processed_rows:
                    key = get_uniqueness_key(row)
                    # 【核心去重逻辑】
                    if key not in all_data:
                        all_data[key] = row
                        newly_added_count += 1
                        print(f"    ➕ 新增文章: {row['标题']}")
                    else:
                        # 如果是重复文章，保留正文更长的版本
                        if len(row['正文']) > len(all_data[key]['正文']):
                            print(f"    🔄 更新文章 (正文更长): {row['标题']}")
                            all_data[key] = row
                        else:
                            print(f"    ➖ 发现重复文章，跳过: {row['标题']}")

                save_counter += newly_added_count
                if save_counter >= save_interval:
                    print(f"\n💾 达到保存阈值，正在保存 {len(all_data)} 条数据...")
                    try:
                        df = pd.DataFrame(list(all_data.values()))
                        df.to_excel(OUT_XLSX, index=False)
                        df.to_csv(TEMP_CSV, index=False, encoding="utf-8-sig")
                        save_counter = 0;
                        print("💾 保存完成！\n")
                    except Exception as e:
                        print(f"⚠️ 文件保存失败: {e}")
            else:
                print("  🆕 本批次无有效新记录")

            page += 1;
            time.sleep(0.5)
        current = prev

    print("\n🎉 完成！正在保存最终数据...")
    try:
        final_df = pd.DataFrame(list(all_data.values()))
        final_df.sort_values(by='时间', ascending=False, inplace=True)
        final_df.to_excel(OUT_XLSX, index=False)
        final_df.to_csv(TEMP_CSV, index=False, encoding="utf-8-sig")
        print(f"🎉 全部完成！文件保存：{OUT_XLSX}")
        print(f"📊 统计：共保存 {len(final_df)} 条不重复的新闻")
    except Exception as e:
        print(f"⚠️ 最终保存失败: {e}")


if __name__ == "__main__":
    main()