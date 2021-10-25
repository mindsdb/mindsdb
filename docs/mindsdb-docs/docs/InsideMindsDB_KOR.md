---
id: inside-mindsdb
title:  MindsdDB 들여다보기
---

다른 트랜잭션 PREDICT, CREATE MODEL 등은 다른 단계/단계를 필요로 하지만 이러한 단계 중 일부를 공유할 수 있습니다. 이 프로세스를 모듈화하기 위해 우리는 트랜잭션 컨트롤러(데이터 버스)의 변수를 통신 인터페이스로 유지합니다. , 버스의 예상 변수가 우선하는 한 주어진 단계의 구현이 변경될 수 있습니다. (다음 섹션에서 일부 Phase Module에 대해 자세히 설명하겠습니다.).

## MindsDB 스택

<iframe width="560" height="315" src="https://www.youtube.com/embed/_eKI3ixBSqs" frameborder="0" allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>

## 데이터 추출기

파일, 디렉토리 및 SQL 호환 데이터베이스와 같은 다양한 데이터 소스에서 입력을 추출하는 작업을 처리합니다. 입력이 쿼리인 경우 모든 암시적 테이블(있는 경우)을 사용하여 조인을 작성합니다.

* **StatsLoader**: 통계 정보가 이미 알려져 있다고 가정하는 PREDICT와 같은 일부 트랜잭션이 있습니다. 올바른 통계를 트랜잭션 BUS에 로드하기만 하면 됩니다..

현재 헤더가 없거나 불완전한 헤더가 있는 {char}sv에서 데이터베이스 로드를 지원하지 않습니다..

**NOTE**: *T현재 mindsDB는 전체 데이터 세트를 메모리에 로드할 수 있어야 합니다. 미래에는 Apache 드릴과 같은 것을 사용하여 매우 큰 데이터 세트를 지원하여 훈련에 필요한 데이터 청크에 대해 FS 또는 db를 쿼리할 수 있습니다. 통계 분석 생성*.


## StatsGenerator

다양한 데이터 소스에서 데이터를 가져와 집계하면 MindsDB가 말뭉치의 각 열에 대한 분석을 실행합니다.

통계 생성기의 목적은 두 가지입니다.:

* 열의 전체 품질을 결정하기 위해 다양한 데이터 품질 점수를 제공하기 위해(예: 분산, 열 간의 일부 상관 메트릭, 중복 양).

* 다음 단계에서 사용해야 하고 모델에 비가 내리기 위해 사용해야 하는 기둥에 대한 속성을 제공합니다. (예: 히스토그램, 데이터 유형)

	After all stats are computed, we warn the user of any interesting insights we found about his data and (if web logs are enabled), use the
generated values to plot some interesting information about the data (e.g. data type distribution, outliers, histogram).

![](https://docs.google.com/drawings/d/e/2PACX-1vTAJo6Zll3jRg-QpZTu2RkXOL0TQXl5dgBHOZqpD3jsW4frhlWxIqc0Mv1OnKbOXNc1cYMFYXMlJ96U/pub?w=502&h=252)

마지막으로 다양한 통계가 메타데이터의 일부로 전달되므로 추가 단계와 모델 자체에서 사용할 수 있습니다.


## Model Interface

* **Train mode**: 'learn'을 호출하면 모델 인터페이스가 모델을 구축하기 위해 훈련을 수행하는 머신 러닝 프레임워크에 데이터를 공급합니다.

* **Predict mode**: 'predict'를 호출할 때 모델 인터페이스는 예측을 생성하기 위해 'learn'이 구축한 모델에 데이터를 공급합니다..

* **Data adaption**: 'ModelInterface' 단계는 단순히 모델에 대한 경량 래퍼입니다. [backends](https://github.com/mindsdb/mindsdb_native/tree/stable/mindsdb_native/libs/backends) which handle adapting the data frame used by mindsdb into a format they can work with. During this process additional metadata for the machine learning libraries/frameworks is generated based on the results of the **Stats Generator** phase.

* **Learning backend**: [앙상블 학습]으로서의 학습 백엔드(https://en.wikipedia.org/wiki/Ensemble_learning) libraries used by mindsdb to train the model that will generate the predictions.
  현재 지원 중인 학습 백엔드는 Lightwood입니다..

## ModelAnalyzer

모델 분석기 단계는 모델에 대한 통찰력을 수집하고 데이터에 대한 통찰력을 수집하기 위해 교육이 완료된 후 실행됩니다.
사후 훈련만 받을 수 있는.

현재 누락된 기능의 수와 예측 값이 속하는 버킷을 기반으로 미래 예측의 정확도를 결정하는 데 사용되는 확률 모델에 대한 피팅이 포함되어 있습니다..
