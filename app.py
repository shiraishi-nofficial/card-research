from chalice import Chalice
import requests
from bs4 import BeautifulSoup
import json
import re
import time
import concurrent.futures
import logging
import random

ROOT_URL = 'https://mldgz4ub9j.execute-api.ap-northeast-1.amazonaws.com/api/'
# ROOT_URL = 'http://127.0.0.1:8000/'
EXECUTION_LAMBDA_URL = ROOT_URL+'execute'

CHUNK_SIZE = 1000
MAX_WORKERS = 50
DC_CHUNK_SIZE = 10
DC_MAX_WORKERS = 30

USER_AGENT_LIST = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:68.0) Gecko/20100101 Firefox/68.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_2_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15'
]
REFERER_LIST = [
    'https://www.google.com',
    'https://www.bing.com',
    'https://www.yahoo.com',
    'https://duckduckgo.com',
    'https://www.baidu.com',
    'https://www.yandex.com',
    'https://www.ask.com',
    'https://www.aol.com',
    'https://www.ecosia.org',
    'https://www.qwant.com'
]
ACCEPT_LANGUAGE_LIST = [
    "en-US,en;q=0.9",
    "ja-JP,ja;q=0.9",
    "es-ES,es;q=0.9",
    # その他のAccept-Languageを追加
]

app = Chalice(app_name='card-research')

# ログ設定を追加
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# @app.route('/')
# def index():
#     return {'hello': 'world'}


@app.route('/', methods=['POST'])
def index():
    input_list = app.current_request.json_body
    result = execute_chunk(input_list, CHUNK_SIZE)
    print(len(result), len(input_list))
    return json.dumps(result)


@app.route('/dc', methods=['POST'])
def index():
    input_list = app.current_request.json_body
    result = execute_chunk(input_list, DC_CHUNK_SIZE)
    print(len(result), len(input_list))
    return json.dumps(result)


@app.route('/execute', methods=['POST'])
def execute():
    try:
        input_body = app.current_request.json_body
        input_list = input_body
        result = execute_with_thread_pool(input_list)
        return {'result': json.dumps(result)}
    except Exception as e:
        logger.error(f"Exception occurred in execute: {e}")
        return {'result': json.dumps({'error': str(e)})}


@app.route('/search', methods=['POST'])
def search_price():
    input_body = app.current_request.json_body
    output_elem = fetch_price(input_body)
    return json.dumps(output_elem)


# 処理速度を計測するデコレータ
def timeit_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"Function {func.__name__} executed in {end_time - start_time:.4f} seconds")
        return result

    return wrapper


# index()関連
def chunk_data(data, chunk_size):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]


def process_chunk(chunk):
    headers = {"Content-Type": "application/json"}
    response = requests.post(EXECUTION_LAMBDA_URL, json=chunk, headers=headers)

    result_dict = json.loads(response.content)

    # エラーハンドリングを追加
    if 'result' in result_dict:
        return json.loads(result_dict['result'])
    else:
        # logger.error(f"Error: 'result' key not found in response: {result_dict}")
        return []


@timeit_decorator
def execute_chunk(data, chunk_size):
    processed_data = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_chunk, chunk) for chunk in chunk_data(data, chunk_size)]
        for future in concurrent.futures.as_completed(futures):
            processed_data.extend(future.result())
    return processed_data


# execute()関連
def execute_with_thread_pool(input_list):
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_elem = {executor.submit(fetch_price, elem): elem for elem in input_list}
        for future in concurrent.futures.as_completed(future_to_elem):
            elem = future_to_elem[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Error processing element {elem}: {e}")
                results.append({'memo': f'Error processing element: {str(e)}'})
    return results


def extract_min_price(price_text):
    # 正規表現を使ってすべての数値を抽出
    # logger.info(f"Extracting min price from text: {price_text}")
    price_numbers = re.findall(r'\d+', price_text.replace(',', ''))

    if price_numbers:
        # 抽出された数値の中で最小値を取得
        min_price = min(int(price) for price in price_numbers)
        # logger.info(f"Extracted min price: {min_price}")
        return min_price
    else:
        return None  # 数値が見つからなかった場合


# search_price()関連
# htmlのタグや要素を返す関数
def find_elem_dict(site):
    if site == 'cr':
        return {'tag': 'span', 'id': 'pricech'}
    elif site == 'ds':
        return {'tag': 'td', 'class_': 'price'}
    elif site == 'hr':
        return {'tag': 'span', 'class_': 'figure'}
    elif site == 'sy':
        return {'tag': 'span', 'class_': 'text-price-detail'}
    # logger.warning(f"Unknown site: {site}")
    return None


# requestsでページのデータを取得し，金額を返す関数
def fetch_price(url_info):
    output_dict = {'site': url_info['site'], 'row': url_info['row']}
    # logger.info(f"Searching price for URL: {url_info['url']}")
    if 'google' in url_info['url']:
        output_dict['price'] = -1
        return output_dict

    if url_info['site'] == 'sy' and 'other' in url_info['url']:
        output_dict['price'] = -32
        output_dict['memo'] = '○円〜△円表記'
        return output_dict

    max_retries = 3
    initial_delay = 1

    for attempt in range(max_retries):
        try:
            headers = {
                'User-Agent': random.choice(USER_AGENT_LIST),
                'Referer': random.choice(REFERER_LIST),
                'Accept-Language': random.choice(ACCEPT_LANGUAGE_LIST)
            }

            response = requests.get(url_info['url'], headers=headers, timeout=5)
            response.encoding = 'utf-8'  # 文字エンコーディングを指定
            # logger.info(f"Received response with status code {response.status_code} for URL: {url_info['url']}")
            if response.status_code == 200:
                # BeautifulSoupで解析
                soup = BeautifulSoup(response.text, 'html.parser')

                # 販売価格を抽出
                elem_dict = find_elem_dict(url_info['site'])
                if elem_dict is None:
                    output_dict['price'] = -6  # 無効なサイト
                    return output_dict
                tag = elem_dict.pop('tag')
                search_conditions = elem_dict
                price_element = soup.find(tag, **search_conditions)

                if price_element:
                    price_text = price_element.text.strip()
                    # 正規表現を使用して数字部分だけを抽出
                    price_number = re.findall(r'\d+', price_text.replace(',', ''))
                    if price_number:
                        if url_info['site'] == 'cr':
                            if '再入荷を知らせる' in soup.get_text():
                                output_dict['price'] = -31
                                output_dict['memo'] = '×'
                                return output_dict
                        if url_info['site'] == 'ds':
                            stock_table = soup.find('table', class_='stock')
                            if stock_table:
                                tr_elements = stock_table.find_all('tr')
                                DEFAULT_MAX_PRICE = -31
                                max_price = DEFAULT_MAX_PRICE
                                for tr in tr_elements:
                                    if '在庫なし' not in tr.get_text():
                                        tr_price_text = tr.find(tag, **search_conditions).text.strip()
                                        tr_price_number = re.findall(r'\d+', tr_price_text.replace(',', ''))[0]
                                        if '状態A' in tr.get_text():  # 状態Aをチェック
                                            if max_price < int(tr_price_number):
                                                max_price = int(tr_price_number)
                                output_dict['price'] = max_price
                                return output_dict
                        if url_info['site'] == 'hr':
                            product_div = soup.find('button', class_='btn_color_emphasis')
                            if '売り切れ' in product_div.get_text():
                                output_dict['price'] = -31
                                output_dict['memo'] = '売り切れ'
                                return output_dict
                        if url_info['site'] == 'sy':
                            if '品切れ中です' in soup.get_text():
                                output_dict['price'] = -31
                                output_dict['memo'] = '品切れ'
                                return output_dict
                        price = int(price_number[0])
                        output_dict['price'] = price
                        output_dict['memo'] = 'success'
                    else:
                        output_dict['price'] = -2  # 数字がなかった
                else:
                    if url_info['site'] == 'sy':  # srの価格幅表示の場合
                        price_element = soup.find('p', class_='price_product')
                        if price_element:
                            price_text = price_element.get_text()
                            price = extract_min_price(price_text)
                            output_dict['price'] = price
                            output_dict['memo'] = 'success-sy'
                        else:
                            output_dict['price'] = -31  # 駿河屋の価格情報がなかった
                    else:
                        output_dict['price'] = -3  # 価格情報がなかった(売り切れ)
            elif response.status_code == 403:
                if attempt < max_retries - 1:
                    logger.warning(f"Received status code 403 for URL: {url_info['url']}. Retrying...")
                    wait_time = initial_delay * (attempt + 1)
                    # time.sleep(wait_time)
                else:
                    output_dict['price'] = -403
                    output_dict['memo'] = response.text
                    logger.error(f"Access forbidden for URL: {url_info['url']} after {max_retries} attempts")
            elif response.status_code == 429:
                if attempt < max_retries - 1:
                    wait_time = initial_delay * (attempt + 1)
                    logger.warning(f"Received status code 429. Retrying in {wait_time} seconds...")
                    # time.sleep(wait_time)
                else:
                    output_dict['price'] = -429
                    output_dict['memo'] = response.text
                    logger.error(f"Too many requests for URL: {url_info['url']}")
            else:
                output_dict['price'] = -4  # ステータスコードが200以外の場合
                output_dict['memo'] = response.status_code
                # logger.error(f"Received status code {response.status_code} for URL: {url_info['url']}")
            if 'price' in output_dict:
                return output_dict
        except requests.exceptions.Timeout:
            logger.error(f"Timeout occurred for URL: {url_info['url']}")
            output_dict['price'] = -5  # タイムアウトエラー
            output_dict['memo'] = 'Timeout error'
            return output_dict  # タイムアウト時もレスポンスを返す
        except Exception as e:
            output_dict['price'] = -10  # 例外が発生した場合
            output_dict['memo'] = str(e)
            logger.error(f"Exception occurred: {e} for URL: {url_info['url']}")
            return output_dict
