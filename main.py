import pandas as pd
import requests
import re
import time

# CSV 파일 경로와 API URL 정의
anime_csv_path = 'anime_korean_dataset.csv'
website_csv_path = 'anime_website.csv'
studio_csv_path = 'anime_studio.csv'
anime_api_url = 'https://api.jikan.moe/v4/anime?q='
external_api_url = 'https://api.jikan.moe/v4/anime/{}/external'

# 특수문자 처리 함수
# 특수 문자 중 '&'는 제거하고, '.'는 그대로 남기고, 영어는 소문자로 변환하는 함수
def preprocess_name(korean_name):
    if not isinstance(korean_name, str):
        return ''
    # 특수 문자 '&' 제거, '.'는 유지
    processed_name = re.sub(r'&', '', korean_name)
    processed_name = re.sub(r',', '', processed_name)
    processed_name = re.sub(r'\[', '', processed_name)
    processed_name = re.sub(r']', '', processed_name)
    processed_name = re.sub(r'【', '', processed_name)
    processed_name = re.sub(r'】', '', processed_name)
    processed_name = re.sub(r'☆', '', processed_name)
    processed_name = re.sub(r'★', '', processed_name)
    processed_name = re.sub(r'~', '', processed_name)
    processed_name = re.sub(r'-', '', processed_name)
    processed_name = re.sub(r':', '', processed_name)
    processed_name = re.sub(r'\'', '', processed_name)
    processed_name = re.sub(r'\"', '', processed_name)
    processed_name = re.sub(r'\(', '', processed_name)
    processed_name = re.sub(r'\)', '', processed_name)
    processed_name = re.sub(r'<', '', processed_name)
    processed_name = re.sub(r'>', '', processed_name)
    processed_name = re.sub(r'『', '', processed_name)
    processed_name = re.sub(r'』', '', processed_name)
    processed_name = re.sub(r'·', '', processed_name)
    processed_name = re.sub(r'♪', '', processed_name)
    processed_name = re.sub(r'_', '', processed_name)
    processed_name = re.sub(r':', '', processed_name)
    # 영어는 소문자로 변환
    processed_name = processed_name.lower()
    return processed_name


# 첫 번째 API 호출 함수
def fetch_anime_data(japanese_name):
    response = requests.get(anime_api_url + japanese_name)
    if response.status_code == 200:
        data = response.json()
        if data['data']:
            for anime in data['data']:
                # airing 조건 체크
                status = anime.get('status')
                anime_type = anime.get('type')
                episodes = anime.get('episodes', 0)
                aired = anime.get('aired', {}).get('from', '')
                year = int(aired[:4]) if aired else None

                if episodes is None:
                    episodes = 0

                if (
                        (status in ['Not yet aired', 'Not available'] and year == 2024) or
                        (status not in ['Not yet aired', 'Not available'])
                ) and (
                        anime_type in ['TV', 'ONA'] or
                        (anime_type == 'OVA' and (episodes >= 3 or episodes == 0))
                ):
                    return anime
        return None
    return None

# 외부 API 호출 함수
def fetch_external_data(mal_id):
    response = requests.get(external_api_url.format(mal_id))
    if response.status_code == 200:
        return response.json().get('data', [])
    return []

# CSV 파일 업데이트 함수
def update_csv_files():
    anime_df = pd.read_csv(anime_csv_path)
    website_df = pd.read_csv(website_csv_path)
    studio_df = pd.read_csv(studio_csv_path)

    # 몇 번째부터 몇 번째까지 작업할지 입력받기
    start_idx = int(input("몇 번째부터 작업을 시작할까요? (0부터 시작): "))
    end_idx = int(input("몇 번째까지 작업할까요? (작업할 마지막 인덱스) (-1을 입력하면 전부): "))
    process_missing_mal_id = input("mal_id가 없는 항목만 처리할까요? (y/n): ").lower()
    flag = False

    if end_idx == -1:
        flag == True

    for idx, row in anime_df.iloc[start_idx:].iterrows():
        try:
            korean_name = row['korean_name']
            english_name = row['english_name']
            japanese_name = row['japaneses_name']
            existing_alternative_titles = row['alternate_titles']

            if process_missing_mal_id == 'y' and pd.notna(mal_id):
                continue  # mal_id가 있으면 스킵

            # API 데이터 가져오기
            api_data = fetch_anime_data(japanese_name)

            if api_data:
                print(korean_name + " 작업 중")
                # english_name이 비어있으면 API에서 채워줌
                if pd.isna(english_name) or english_name == '':
                    english_name = api_data.get('title_english') or api_data.get('title')
                    anime_df.at[idx, 'english_name'] = english_name

                # API 데이터를 기반으로 기존 데이터 업데이트
                mal_id = api_data.get('mal_id')
                print(mal_id)
                anime_df.at[idx, 'mal_id'] = mal_id or row['mal_id']
                anime_df.at[idx, 'rating'] = api_data.get('rating') or row['rating']
                anime_df.at[idx, 'production_company'] = ', '.join(
                    [studio['name'] for studio in api_data.get('studios', [])]) or row['production_company']
                anime_df.at[idx, 'genre'] = ', '.join([genre['name'] for genre in api_data.get('genres', [])]) or row[
                    'genre']
                anime_df.at[idx, 'themes'] = ', '.join([theme['name'] for theme in api_data.get('themes', [])]) or row[
                    'themes']
                anime_df.at[idx, 'demographics'] = ', '.join(
                    [demo['name'] for demo in api_data.get('demographics', [])]) or row['demographics']
                anime_df.at[idx, 'small_image_url'] = api_data.get('images', {}).get('jpg', {}).get('small_image_url',
                                                                                                    row[
                                                                                                        'small_image_url'])
                anime_df.at[idx, 'large_image_url'] = api_data.get('images', {}).get('jpg', {}).get('large_image_url',
                                                                                                    row[
                                                                                                        'large_image_url'])
                anime_df.at[idx, 'image_url'] = api_data.get('images', {}).get('jpg', {}).get('image_url',
                                                                                              row['image_url'])
                anime_df.at[idx, 'trailer_url'] = api_data.get('trailer', {}).get('url', row['trailer_url'])
                anime_df.at[idx, 'score'] = api_data.get('score', row['score'])
                anime_df.at[idx, 'scored_by'] = api_data.get('scored_by', row['scored_by'])
                anime_df.at[idx, 'members'] = api_data.get('members', row['members'])
                anime_df.at[idx, 'mal_id'] = api_data.get('mal_id', row['mal_id'])
                anime_df.at[idx, 'animelist_url'] = api_data.get('url', row['animelist_url'])
                anime_df.at[idx, 'year'] = api_data.get('year', row['year'])
                anime_df.at[idx, 'favorites'] = api_data.get('favorites', row['favorites'])
                anime_df.at[idx, 'check_sum'] = api_data.get('title', row['check_sum'])
                anime_df.at[idx, 'anime_type'] = api_data.get('type', row['anime_type'])
                anime_df.at[idx, 'episodes'] = api_data.get('episodes', row['episodes'])
                anime_df.at[idx, 'season'] = api_data.get('season', row['season'])
                anime_df.at[idx, 'source'] = api_data.get('source', row['source'])

                # korean_name 전처리 후 alternative_title에 추가
                processed_korean_name = preprocess_name(korean_name)
                processed_english_name = preprocess_name(english_name)
                processed_japanese_name = preprocess_name(japanese_name)

                # 기존 alternative_titles가 있다면 배열로 나누고, 전처리된 korean_name 추가
                if pd.notna(existing_alternative_titles):
                    alternative_titles = existing_alternative_titles.split(',')
                elif existing_alternative_titles == '':
                    alternative_titles = []
                else:
                    alternative_titles = []

                if korean_name not in alternative_titles:
                    alternative_titles.append(korean_name)

                if processed_korean_name not in alternative_titles:
                    alternative_titles.append(processed_korean_name)

                if processed_english_name not in alternative_titles:
                    alternative_titles.append(processed_english_name)

                if processed_japanese_name not in alternative_titles:
                    alternative_titles.append(processed_japanese_name)

                if korean_name not in alternative_titles:
                    alternative_titles.append(korean_name)

                anime_df.at[idx, 'alternate_titles'] = ','.join(alternative_titles)

                # Studio 정보 처리
                for studio in api_data.get('studios', []):
                    studio_id = studio['mal_id']
                    if studio_id not in studio_df['mal_id'].values:
                        new_studio = pd.DataFrame({
                            'mal_id': [studio_id],
                            'type': [studio.get('type')],
                            'name': [studio.get('name')],
                            'url': [studio.get('url')],
                            'korean_name': ['']
                        })
                        studio_df = pd.concat([studio_df, new_studio], ignore_index=True)

                # 외부 사이트 정보 처리
                external_data = fetch_external_data(mal_id)
                for external_site in external_data:
                    if website_df[(website_df['mal_id'] == mal_id) & (website_df['name'] == external_site['name'])].empty:
                        new_website = pd.DataFrame({
                            'mal_id': [mal_id],
                            'anime_name': [korean_name],
                            'name': [external_site['name']],
                            'url': [external_site['url']]
                        })
                        website_df = pd.concat([website_df, new_website], ignore_index=True)

            if idx == end_idx and flag == False:
                break

        except Exception as e:
            print(f"오류 발생: {korean_name}, 에러 메시지: {str(e)}")
            continue

    # NaN 값이 있는 열에 대해 기본값을 채우는 방법 (필요시)
    anime_df['score'] = anime_df['score'].fillna(0)
    anime_df['scored_by'] = pd.to_numeric(anime_df['scored_by'], errors='coerce').fillna(0)
    anime_df['members'] = pd.to_numeric(anime_df['members'], errors='coerce').fillna(0)
    anime_df['favorites'] = pd.to_numeric(anime_df['favorites'], errors='coerce').fillna(0)
    anime_df['episodes'] = pd.to_numeric(anime_df['episodes'], errors='coerce').fillna(0)

    # astype(Int64)로 변환 (NaN을 처리할 수 있는 Int64 타입)
    anime_df['year'] = pd.to_numeric(anime_df['year'], errors='coerce').replace(pd.NA, '').astype('Int64')
    anime_df['scored_by'] = anime_df['scored_by'].astype('Int64')
    anime_df['members'] = anime_df['members'].astype('Int64')
    anime_df['favorites'] = anime_df['favorites'].astype('Int64')
    anime_df['episodes'] = anime_df['episodes'].astype('Int64')
    anime_df['mal_id'] = anime_df['mal_id'].astype('Int64')

    # 'nan' 문자열을 빈 문자열로 변환
    anime_df = anime_df.replace('nan', '')

    # studio_df와 website_df에서도 동일하게 처리
    studio_df['mal_id'] = studio_df['mal_id'].astype('Int64')
    website_df['mal_id'] = website_df['mal_id'].astype('Int64')

    studio_df = studio_df.replace('nan', '')
    website_df = website_df.replace('nan', '')

    anime_df.to_csv('anime_korean_dataset_updated_fixed.csv', index=False, encoding='utf-8-sig')
    studio_df.to_csv('anime_studio.csv', index=False, encoding='utf-8-sig')
    website_df.to_csv('anime_website.csv', index=False, encoding='utf-8-sig')

    print("작업 완료!")

# 함수 실행
update_csv_files()
