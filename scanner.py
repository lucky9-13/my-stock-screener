import os
import json
import logging
from datetime import datetime
import pandas as pd
import FinanceDataReader as fdr
import asyncio
import aiohttp
from bs4 import BeautifulSoup

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def fetch_naver_finance(session, code):
    """네이버 금융에서 특정 종목의 PER, PBR을 비동기로 스크래핑합니다."""
    url = f"https://finance.naver.com/item/main.naver?code={code}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        async with session.get(url, headers=headers, timeout=10) as response:
            if response.status != 200:
                return code, 0.0, 0.0
                
            text = await response.text()
            soup = BeautifulSoup(text, 'lxml')
            
            per = 0.0
            pbr = 0.0
            
            per_em = soup.select_one('#_per')
            pbr_em = soup.select_one('#_pbr')
            
            if per_em:
                try:
                    per = float(per_em.text.replace(',', '').strip())
                except ValueError:
                    pass
                    
            if pbr_em:
                try:
                    pbr = float(pbr_em.text.replace(',', '').strip())
                except ValueError:
                    pass
                    
            return code, per, pbr

    except Exception as e:
        # logging.debug(f"[{code}] 스크래핑 오류: {e}")
        return code, 0.0, 0.0

async def process_market(market, max_concurrent_requests=50):
    """특정 시장(KOSPI/KOSDAQ)의 종목 데이터를 비동기로 수집합니다."""
    logging.info(f"{market} 데이터 수집 준비 중...")
    
    try:
        df_listing = fdr.StockListing(market)
        market_data = []
        
        # 주식 기본 정보 추출 (동기)
        for _, row in df_listing.iterrows():
            ticker = str(row['Code'])
            name = str(row['Name'])
            price = int(row['Close']) if pd.notna(row['Close']) else 0
            
            market_cap_raw = int(row['Marcap']) if 'Marcap' in row and pd.notna(row['Marcap']) else 0
            market_cap_trillion = market_cap_raw / 1_000_000_000_000
            
            market_data.append({
                "code": ticker,
                "name": name,
                "market": market,
                "price": price,
                "per": 0.0, # 나중에 채움
                "pbr": 0.0, # 나중에 채움
                "market_cap": round(market_cap_trillion, 2)
            })
            
        logging.info(f"{market} 기본 정보 수집 완료. 총 {len(market_data)} 종목 PER/PBR 스크래핑 시작...")
        
        # 세마포어를 이용해 동시 요청 수 제한
        sem = asyncio.Semaphore(max_concurrent_requests)
        
        async def fetch_with_sem(session, code):
            async with sem:
                return await fetch_naver_finance(session, code)
        
        # 비동기로 PER/PBR 스크래핑
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_with_sem(session, item['code']) for item in market_data]
            
            # 진행 상황 로깅을 위해 tqdm 대신 수동 청크 처리 (단순화)
            results = {}
            total = len(tasks)
            completed = 0
            
            chunk_size = 500
            # 작업을 청크 단위로 나누어 실행하여 중간중간 로깅
            for i in range(0, total, chunk_size):
                chunk = tasks[i:i + chunk_size]
                chunk_results = await asyncio.gather(*chunk)
                
                for code, per, pbr in chunk_results:
                    results[code] = (per, pbr)
                    
                completed += len(chunk)
                logging.info(f"{market} 스크래핑 진행률: {completed}/{total}")
                
                # 네이버 서버 부하 방지를 위해 짧은 대기
                await asyncio.sleep(1)

        # 결과 합치기
        for item in market_data:
            code = item['code']
            if code in results:
                item['per'], item['pbr'] = results[code]
                
        return market_data

    except Exception as e:
        logging.error(f"{market} 데이터 수집 중 에러: {e}")
        return []

async def fetch_all_stock_data():
    """모든 시장의 데이터를 수집합니다."""
    # KOSPI와 KOSDAQ을 순차적으로 수집 (메모리와 연결 안정성 확보)
    kospi_data = await process_market("KOSPI")
    kosdaq_data = await process_market("KOSDAQ")
    
    return kospi_data + kosdaq_data

def save_to_json(data, filepath):
    """수집된 데이터를 JSON 파일로 저장합니다."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    # 시간 포맷: YYYY-MM-DD HH:mm
    current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M')
    
    # 데이터를 리스트 대신 딕셔너리로 감싸서 시간 정보와 함께 저장
    output_data = {
        "generation_time": current_time_str,
        "stocks": data
    }
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    logging.info(f"데이터가 {filepath} 에 저장되었습니다. (총 {len(data)} 종목, 시간: {current_time_str})")

def main():
    start_time = datetime.now()
    logging.info("비동기 주식 데이터 수집 시작 (FinanceDataReader + aiohttp)")
    
    try:
        if os.name == 'nt':
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            
        stock_data = asyncio.run(fetch_all_stock_data())
        
        output_file = os.path.join(os.path.dirname(__file__), 'data', 'stock_data.json')
        save_to_json(stock_data, output_file)
        
    except Exception as e:
        logging.error(f"오류 발생: {e}")
        
    end_time = datetime.now()
    logging.info(f"주식 데이터 수집 완료 (소요시간: {end_time - start_time})")

if __name__ == "__main__":
    main()
