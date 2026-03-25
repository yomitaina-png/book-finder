"""
まちの本屋さん — Notion週次更新スクリプト
"""

import os
import re
import time
import requests
from bs4 import BeautifulSoup

# =====================================================
# 設定
# =====================================================
NOTION_TOKEN = os.environ.get('NOTION_TOKEN', '')
NOTION_DB_ID = os.environ.get('NOTION_DB_ID', '')

NIPPAN_URL  = 'https://www.nippan.co.jp/ranking/weekly/'
NDL_API_URL = 'https://ndlsearch.ndl.go.jp/api/opensearch'
NOTION_API  = 'https://api.notion.com/v1'

HEADERS_NOTION = {
    'Authorization': f'Bearer {NOTION_TOKEN}',
    'Content-Type': 'application/json',
    'Notion-Version': '2022-06-28',
}

HEADERS_WEB = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

# =====================================================
# ユーティリティ：リトライ付きリクエスト
# =====================================================
def request_with_retry(method, url, retries=5, wait=10, **kwargs):
    """タイムアウト時に自動リトライするリクエスト"""
    for attempt in range(1, retries + 1):
        try:
            res = requests.request(method, url, timeout=60, **kwargs)
            res.raise_for_status()
            return res
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            print(f'    ⏳ 接続エラー（試行{attempt}/{retries}）: {e}')
            if attempt < retries:
                print(f'    {wait}秒後に再試行...')
                time.sleep(wait)
            else:
                raise
        except requests.exceptions.HTTPError as e:
            # 429 レート制限の場合は長めに待つ
            if e.response.status_code == 429:
                retry_after = int(e.response.headers.get('Retry-After', 30))
                print(f'    ⏳ レート制限。{retry_after}秒待機...')
                time.sleep(retry_after)
            else:
                raise
    return None

# =====================================================
# STEP 1: 日販ページからランキングを取得
# =====================================================
def fetch_nippan_ranking():
    print('📥 日販ランキングを取得中...')
    res = request_with_retry('GET', NIPPAN_URL, headers=HEADERS_WEB)
    soup = BeautifulSoup(res.text, 'html.parser')

    books = []
    table = soup.find('table')
    if not table:
        raise ValueError('日販ページのテーブルが見つかりません')

    rows = table.find_all('tr')[1:]

    for row in rows:
        cols = row.find_all('td')
        if len(cols) < 5:
            continue

        rank_text      = cols[0].get_text(strip=True)
        prev_rank_text = cols[1].get_text(strip=True)

        title_tag = cols[3].find('a')
        title = title_tag.get_text(strip=True) if title_tag else cols[3].get_text(strip=True)

        author    = cols[4].get_text(strip=True) if len(cols) > 4 else ''
        price     = cols[5].get_text(strip=True) if len(cols) > 5 else ''
        publisher = cols[6].get_text(strip=True) if len(cols) > 6 else ''

        img_tag   = cols[2].find('img') if len(cols) > 2 else None
        cover_url = img_tag['src'] if img_tag and img_tag.get('src') else ''
        if cover_url and cover_url.startswith('/'):
            cover_url = 'https://www.nippan.co.jp' + cover_url

        rank_num = int(re.sub(r'[^0-9]', '', rank_text)) if re.sub(r'[^0-9]', '', rank_text) else 0
        prev_str = re.sub(r'[^0-9\-]', '', prev_rank_text) or '-'

        if rank_num == 0 or not title:
            continue

        books.append({
            'rank':      rank_num,
            'prevRank':  prev_str,
            'title':     title,
            'author':    author,
            'price':     price,
            'publisher': publisher,
            'coverUrl':  cover_url,
            'isbn':      '',
            'synopsis':  '',
        })

    print(f'  → {len(books)}冊取得')
    return books

# =====================================================
# STEP 2: 国立国会図書館APIでISBNを取得
# =====================================================
def fetch_isbn(title):
    try:
        res = requests.get(
            NDL_API_URL,
            params={'title': title, 'cnt': 3, 'mediatype': 1},
            headers=HEADERS_WEB,
            timeout=15
        )
        if not res.ok:
            return ''
        soup = BeautifulSoup(res.text, 'xml')
        for item in soup.find_all('item'):
            for identifier in item.find_all('identifier'):
                m = re.search(r'978\d{10}', identifier.get_text())
                if m:
                    return m.group()
    except Exception as e:
        print(f'    NDL APIエラー: {e}')
    return ''

def enrich_with_isbn(books):
    print('🔍 ISBNを取得中...')
    for i, book in enumerate(books):
        isbn = fetch_isbn(book['title'])
        book['isbn'] = isbn
        print(f"  [{i+1:2d}] {book['title'][:25]:<25} → {isbn or '未取得'}")
        time.sleep(0.5)
    return books

# =====================================================
# STEP 3: Notionデータベースを更新
# =====================================================
def get_existing_pages():
    pages = []
    cursor = None
    while True:
        body = {'page_size': 100}
        if cursor:
            body['start_cursor'] = cursor
        res = request_with_retry(
            'POST',
            f'{NOTION_API}/databases/{NOTION_DB_ID}/query',
            headers=HEADERS_NOTION,
            json=body
        )
        data = res.json()
        pages.extend(data.get('results', []))
        if not data.get('has_more'):
            break
        cursor = data.get('next_cursor')
    return pages

def delete_page(page_id):
    try:
        request_with_retry(
            'PATCH',
            f'{NOTION_API}/pages/{page_id}',
            headers=HEADERS_NOTION,
            json={'archived': True}
        )
    except Exception as e:
        print(f'    削除エラー: {e}')

def create_page(book):
    properties = {
        '書名': {
            'title': [{'text': {'content': book['title'][:2000]}}]
        },
        '順位': {
            'number': book['rank']
        },
        '前週順位': {
            'rich_text': [{'text': {'content': book['prevRank']}}]
        },
        '著者名': {
            'rich_text': [{'text': {'content': book['author'][:2000]}}]
        },
        '価格': {
            'rich_text': [{'text': {'content': book['price']}}]
        },
        '出版社': {
            'rich_text': [{'text': {'content': book['publisher'][:2000]}}]
        },
        'ISBNコード': {
            'rich_text': [{'text': {'content': book['isbn']}}]
        },
        'あらすじ': {
            'rich_text': [{'text': {'content': book['synopsis'][:2000]}}]
        },
    }
    if book['coverUrl']:
        properties['書影URL'] = {'url': book['coverUrl']}

    try:
        request_with_retry(
            'POST',
            f'{NOTION_API}/pages',
            headers=HEADERS_NOTION,
            json={
                'parent': {'database_id': NOTION_DB_ID},
                'properties': properties,
            }
        )
        return True
    except Exception as e:
        print(f'    ページ作成エラー: {e}')
        return False

def update_notion(books):
    print('🗑️  既存データを削除中...')
    existing = get_existing_pages()
    for page in existing:
        delete_page(page['id'])
        time.sleep(0.3)
    print(f'  → {len(existing)}件削除')

    print('✍️  新しいデータを書き込み中...')
    success = 0
    for book in books:
        if create_page(book):
            success += 1
            print(f"  ✅ {book['rank']}位: {book['title'][:30]}")
        time.sleep(0.5)  # Notion APIのレート制限対策
    print(f'  → {success}/{len(books)}件書き込み完了')

# =====================================================
# メイン処理
# =====================================================
def main():
    print('=' * 50)
    print('まちの本屋さん — Notion週次更新')
    print('=' * 50)

    if not NOTION_TOKEN or not NOTION_DB_ID:
        raise ValueError('環境変数 NOTION_TOKEN と NOTION_DB_ID を設定してください')

    books = fetch_nippan_ranking()
    books = enrich_with_isbn(books)
    update_notion(books)

    print('=' * 50)
    print('✅ 更新完了！')
    print('=' * 50)

if __name__ == '__main__':
    main()
