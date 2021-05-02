PyTrader는 시스템 트레이딩을 학습하는 과정에서 만들어진 프로그램입니다.
이 프로그램은 파이썬3, PyQt5을 기반으로 제작되었으며,
키움증권의 OpenApi+ 를 사용하고 있습니다.



# 날강두 트레이더

1. code를 특정 폴더에 clone
2. order.db를 예) D:\stock\database 에 카피한다.
3. Main.py를 실행하면 일단 동작은 됨
4. 모의투자를 하면서 알고리즘을 채워 넣으면 자동 봇을 사용 할 수 있습니다.


### Main.py
- buy, sell thread를 만들고 실행 
- 두 threads간의 통신을 연결

### buy
- 매매 기법함수를 실행하는 thread

### sell
- 보유 종목을 monitor
- 시간에 맞춰 매수 알고리즘을 실행

### parameter.json
- 사용자의 input을 저장하는 
