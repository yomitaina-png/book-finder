"""
まちの本屋さん — Notion週次更新スクリプト（列名自動対応版）
"""

import os
import re
import time
import json
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
# リトライ付きリクエスト
# =====================================================
def request_with_retry(method, url, retries=5, wait=10, **kwargs):
    for attempt in range(1, retries + 1):
        try:
            res = requests.request(method, url, timeout=60, **kwargs)
            res.raise_for_status()
            return res
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            print(f'    ⏳ 接続エラー（試行{attempt}/{retries}）: {e}')
            if attempt < retries:
                time.sleep(wait)
            else:
                raise
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                retry_after = int(e.response.headers.get('Retry-After', 30))
                print(f'    ⏳ レート制限。{retry_after}秒待機...')
                time.sleep(retry_after)
            else:
                raise

# =====================================================
# STEP 0: Notionの列名と型を取得して表示
# =====================================================
def get_db_properties():
    """データベースの列名と型を取得"""
    print('🔍 Notionデータベースの列名を確認中...')
    res = request_with_retry(
        'GET',
        f'{NOTION_API}/databases/{NOTION_DB_ID}',
        headers=HEADERS_NOTION
    )
    props = res.json().get('properties', {})
    print('  === 実際の列名一覧 ===')
    for name, prop in props.items():
        print(f'  列名: "{name}"  型: {prop["type"]}')
    print()
    return props

def build_prop_map(props):
    """列名のキーワードマッチングで自動マッピング"""
    mapping = {
        'title_col':     None,  # タイトル型（書名）
        'rank_col':      None,  # 順位
        'prev_col':      None,  # 前週順位
        'author_col':    None,  # 著者
        'price_col':     None,  # 価格・定価
        'publisher_col': None,  # 出版社
        'cover_col':     None,  # 書影URL
        'detail_col':    None,  # 商品詳細URL
        'isbn_col':      None,  # ISBN
        'synopsis_col':  None,  # あらすじ・商品概要
    }

    for name, prop in props.items():
        t = prop['type']
        n = name.lower()

        if t == 'title':
            mapping['title_col'] = name
        elif '順位' in name and '前週' not in name and '先週' not in name:
            mapping['rank_col'] = name
        elif '前週' in name or '先週' in name:
            mapping['prev_col'] = name
        elif '著者' in name or '編者' in name or 'author' in n:
            mapping['author_col'] = name
        elif '価格' in name or '定価' in name or '値段' in name or 'price' in n:
            mapping['price_col'] = name
        elif '出版社' in name or 'publisher' in n:
            mapping['publisher_col'] = name
        elif '書影' in name or 'cover' in n:
            mapping['cover_col'] = name
        elif '詳細' in name or 'detail' in n:
            mapping['detail_col'] = name
        elif 'isbn' in n or 'ｉｓｂｎ' in name:
            mapping['isbn_col'] = name
        elif 'あらすじ' in name or '概要' in name or 'synopsis' in n or '紹介' in name:
            mapping['synopsis_col'] = name

    print('  === 自動マッピング結果 ===')
    for key, val in mapping.items():
        print(f'  {key}: "{val}"')
    print()
    return mapping

# =====================================================
# STEP 1: 日販ランキング取得
# =====================================================
def fetch_nippan_ranking():
    print('📥 日販ランキングを取得中...')
    res = request_with_retry('GET', NIPPAN_URL, headers=HEADERS_WEB)
    soup = BeautifulSoup(res.text, 'html.parser')

    books = []
    table = soup.find('table')
    if not table:
        raise ValueError('テーブルが見つかりません')

    for row in table.find_all('tr')[1:]:
        cols = row.find_all('td')
        if len(cols) < 5:
            continue

        rank_text = cols[0].get_text(strip=True)
        prev_text = cols[1].get_text(strip=True)

        title_tag  = cols[3].find('a')
        title      = title_tag.get_text(strip=True) if title_tag else cols[3].get_text(strip=True)
        detail_url = title_tag['href'] if title_tag and title_tag.get('href') else ''
        if detail_url and detail_url.startswith('/'):
            detail_url = 'https://www.nippan.co.jp' + detail_url

        author    = cols[4].get_text(strip=True) if len(cols) > 4 else ''
        price     = cols[5].get_text(strip=True) if len(cols) > 5 else ''
        publisher = cols[6].get_text(strip=True) if len(cols) > 6 else ''

        img_tag   = cols[2].find('img') if len(cols) > 2 else None
        cover_url = img_tag['src'] if img_tag and img_tag.get('src') else ''
        if cover_url and cover_url.startswith('/'):
            cover_url = 'https://www.nippan.co.jp' + cover_url

        rank_num = int(re.sub(r'[^0-9]', '', rank_text)) if re.sub(r'[^0-9]', '', rank_text) else 0
        prev_str = re.sub(r'[^0-9\-]', '', prev_text) or '-'

        if rank_num == 0 or not title:
            continue

        books.append({
            'rank': rank_num, 'prevRank': prev_str,
            'title': title, 'author': author,
            'price': price, 'publisher': publisher,
            'coverUrl': cover_url, 'detailUrl': detail_url,
            'isbn': '', 'synopsis': '',
        })

    print(f'  → {len(books)}冊取得')
    return books

# =====================================================
# STEP 2: ISBN取得
# =====================================================
def fetch_isbn(title):
    try:
        res = requests.get(NDL_API_URL,
            params={'title': title, 'cnt': 3, 'mediatype': 1},
            headers=HEADERS_WEB, timeout=15)
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
# STEP 3: Notion更新
# =====================================================
def get_existing_pages():
    pages = []
    cursor = None
    while True:
        body = {'page_size': 100}
        if cursor:
            body['start_cursor'] = cursor
        res = request_with_retry('POST',
            f'{NOTION_API}/databases/{NOTION_DB_ID}/query',
            headers=HEADERS_NOTION, json=body)
        data = res.json()
        pages.extend(data.get('results', []))
        if not data.get('has_more'):
            break
        cursor = data.get('next_cursor')
    return pages

def delete_page(page_id):
    try:
        request_with_retry('PATCH', f'{NOTION_API}/pages/{page_id}',
            headers=HEADERS_NOTION, json={'archived': True})
    except Exception as e:
        print(f'    削除エラー: {e}')

def make_text(value):
    return {'rich_text': [{'text': {'content': str(value)[:2000]}}]}

def create_page(book, mapping, props):
    properties = {}

    # タイトル列（必須）
    if mapping['title_col']:
        properties[mapping['title_col']] = {
            'title': [{'text': {'content': book['title'][:2000]}}]
        }

    # 順位：数値型 or テキスト型に応じて切り替え
    if mapping['rank_col']:
        col_type = props[mapping['rank_col']]['type']
        if col_type == 'number':
            properties[mapping['rank_col']] = {'number': book['rank']}
        else:
            properties[mapping['rank_col']] = make_text(book['rank'])

    # テキスト系列
    text_fields = [
        ('prev_col',      book['prevRank']),
        ('author_col',    book['author']),
        ('price_col',     book['price']),
        ('publisher_col', book['publisher']),
        ('isbn_col',      book['isbn']),
        ('synopsis_col',  book['synopsis']),
    ]
    for key, value in text_fields:
        col = mapping[key]
        if col and value:
            properties[col] = make_text(value)

    # URL型列
    if mapping['cover_col'] and book['coverUrl']:
        properties[mapping['cover_col']] = {'url': book['coverUrl']}
    if mapping['detail_col'] and book['detailUrl']:
        properties[mapping['detail_col']] = {'url': book['detailUrl']}

    try:
        res = request_with_retry('POST', f'{NOTION_API}/pages',
            headers=HEADERS_NOTION,
            json={'parent': {'database_id': NOTION_DB_ID}, 'properties': properties})
        return True
    except requests.exceptions.HTTPError as e:
        # 詳細なエラー内容を表示
        print(f'    ページ作成エラー: {e.response.status_code}')
        print(f'    詳細: {e.response.text[:500]}')
        return False

def update_notion(books, mapping, props):
    print('🗑️  既存データを削除中...')
    existing = get_existing_pages()
    for page in existing:
        delete_page(page['id'])
        time.sleep(0.3)
    print(f'  → {len(existing)}件削除')

    print('✍️  新しいデータを書き込み中...')
    success = 0
    for book in books:
        if create_page(book, mapping, props):
            success += 1
            print(f"  ✅ {book['rank']}位: {book['title'][:30]}")
        time.sleep(0.5)
    print(f'  → {success}/{len(books)}件書き込み完了')

# =====================================================
# メイン
# =====================================================
def main():
    print('=' * 50)
    print('まちの本屋さん — Notion週次更新')
    print('=' * 50)

    if not NOTION_TOKEN or not NOTION_DB_ID:
        raise ValueError('環境変数 NOTION_TOKEN と NOTION_DB_ID を設定してください')

    # 列名を自動取得・マッピング
    props = get_db_properties()
    mapping = build_prop_map(props)

    books = fetch_nippan_ranking()
    books = enrich_with_isbn(books)
    update_notion(books, mapping, props)

    print('=' * 50)
    print('✅ 更新完了！')
    print('=' * 50)

if __name__ == '__main__':
    main()
