"""
デバッグ用：Notionデータベースの列名を確認するスクリプト
GitHubのupdate_notion.pyと同じフォルダに置いて実行してください

実行方法:
    python check_notion_columns.py
"""

import os
import requests
import json

NOTION_TOKEN = os.environ.get('NOTION_TOKEN', '')
NOTION_DB_ID = os.environ.get('NOTION_DB_ID', '')

if not NOTION_TOKEN or not NOTION_DB_ID:
    # 環境変数がない場合は直接入力
    NOTION_TOKEN = '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'
    NOTION_DB_ID = '32d63f88fff780cf9a0decdcddc799e7'

res = requests.get(
    f'https://api.notion.com/v1/databases/{NOTION_DB_ID}',
    headers={
        'Authorization': f'Bearer {NOTION_TOKEN}',
        'Notion-Version': '2022-06-28',
    },
    timeout=30
)

print(f'ステータス: {res.status_code}')
data = res.json()

print('\n=== Notionデータベースの列名一覧 ===')
for name, prop in data.get('properties', {}).items():
    print(f'  列名: "{name}"  型: {prop["type"]}')
