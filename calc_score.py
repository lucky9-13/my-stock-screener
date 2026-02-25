import pandas as pd
import FinanceDataReader as fdr
import requests
from bs4 import BeautifulSoup
import time

def parse_float(val_str):
    if not val_str: return 0.0
    val_str = val_str.replace(',', '').strip()
    if val_str in ['N/A', '-', '', '완전잠식']: return 0.0
    try:
        return float(val_str)
    except:
        return 0.0

def get_naver_financials(code):
    url = f"https://finance.naver.com/item/main.naver?code={code}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    per = 0.0
    pbr = 0.0
    roe = 0.0
    opm = 0.0 # 영업이익률
    debt = 0.0 # 부채비율
    
    try:
        res = requests.get(url, headers=headers, timeout=5)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'lxml')
        
        # PER, PBR from main summary
        per_em = soup.select_one('#_per')
        pbr_em = soup.select_one('#_pbr')
        
        if per_em: per = parse_float(per_em.text)
        if pbr_em: pbr = parse_float(pbr_em.text)
        
        # Financial table
        table = soup.select_one('table.tb_type1.tb_num.tb_type1_ifrs')
        if table:
            rows = table.select('tbody tr')
            for row in rows:
                th = row.select_one('th')
                if not th: continue
                title = th.text.strip()
                
                tds = row.select('td')
                val = '0'
                # Find the latest annual/quarterly non-empty value
                for td in reversed(tds[0:4]):
                    text = td.text.strip().replace(',', '')
                    if text and text not in ['-', 'N/A']:
                        val = text
                        break
                        
                if '영업이익률' in title:
                    opm = parse_float(val)
                elif 'ROE' in title:
                    roe = parse_float(val)
                elif '부채비율' in title:
                    if '완전잠식' in val:
                        debt = 999.0
                    else:
                        debt_val = parse_float(val)
                        if debt_val == 0.0 and val not in ['0', '0.0']:
                            debt = 999.0
                        else:
                            debt = debt_val
        else:
            debt = 999.0 # if no data

        if debt == 0.0: # fallback if missing
             debt = 999.0
             
        return per, pbr, roe, opm, debt
    except Exception as e:
        return 0.0, 0.0, 0.0, 0.0, 999.0

def main():
    kospi = fdr.StockListing('KOSPI')
    kosdaq = fdr.StockListing('KOSDAQ')
    
    combined = pd.concat([kospi, kosdaq])
    # Top 50 by Market Cap
    top_50 = combined.sort_values(by='Marcap', ascending=False).head(50)
    
    results = []
    
    for _, row in top_50.iterrows():
        code = str(row['Code']).zfill(6)
        name = row['Name']
        
        per, pbr, roe, opm, debt = get_naver_financials(code)
        
        # PER Score (PER 5 이하면 만점, 25 이상이면 0점, 음수/0은 0점)
        if per <= 0:
            per_score = 0.0
        else:
            per_score = max(0.0, min(20.0, (25 - per) / 20 * 20))
            
        # PBR Score (PBR 0.5 이하면 만점, 2.5 이상이면 0점, 음수/0은 0점)
        if pbr <= 0:
            pbr_score = 0.0
        else:
            pbr_score = max(0.0, min(20.0, (2.5 - pbr) / 2 * 20))
            
        # ROE Score (ROE 20% 이상 만점, 0% 이하 0점)
        roe_score = max(0.0, min(20.0, roe / 20 * 20))
        
        # OPM Score (영업이익률 20% 이상 만점, 0% 이하 0점)
        opm_score = max(0.0, min(20.0, opm / 20 * 20))
        
        # Debt Score (부채비율 50% 이하 만점, 250% 이상 0점)
        if debt >= 250 or debt == 999.0:
            debt_score = 0.0
        else:
            debt_score = max(0.0, min(20.0, (250 - debt) / 200 * 20))
            
        total_score = per_score + pbr_score + roe_score + opm_score + debt_score
        
        results.append({
            'name': name,
            'per': per,
            'pbr': pbr,
            'roe': roe,
            'opm': opm,
            'debt': debt if debt != 999.0 else 'N/A',
            'score': round(total_score, 2)
        })
        time.sleep(0.1) # Be nice to Naver Servers
        
    results.sort(key=lambda x: x['score'], reverse=True)
    
    print("| 순위 | 종목명 | 총점 | PER | PBR | ROE(%) | 영업이익률(%) | 부채비율(%) |")
    print("|---|---|---|---|---|---|---|---|")
    for idx, r in enumerate(results):
        print(f"| {idx+1} | {r['name']} | **{r['score']}** | {r['per']} | {r['pbr']} | {r['roe']} | {r['opm']} | {r['debt']} |")

if __name__ == '__main__':
    main()
