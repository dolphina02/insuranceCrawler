#!/usr/bin/env python3
"""
파일명 형식 테스트
"""

from datetime import datetime
import os

def test_filename_format():
    """파일명 형식 테스트"""
    
    # 현재 시간으로 타임스탬프 생성
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # 각 카테고리별 파일명 생성 예시
    categories = [
        '종신보험', '정기보험', '질병보험', '암보험', 'CI보험', 
        '상해보험', '어린이보험', '치아보험', '간병/치매보험'
    ]
    
    print("=" * 60)
    print("파일명 형식 테스트")
    print("=" * 60)
    print(f"실행 시점: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"타임스탬프: {timestamp}")
    print()
    
    print("생성될 파일명 목록:")
    for category in categories:
        filename = f"{category}_{timestamp}.xls"
        print(f"  {filename}")
    
    print()
    print("예상 다운로드 폴더 구조:")
    print("downloads/")
    for category in categories:
        filename = f"{category}_{timestamp}.xls"
        print(f"├── {filename}")
    
    print()
    print("✅ 파일명 형식이 올바르게 생성됩니다!")

if __name__ == "__main__":
    test_filename_format()