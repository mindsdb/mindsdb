# What is MindsDB?
# MindsDB는 무엇인가 ?

MindsDB를 사용하면 데이터베이스에서 직접 고급 예측 기능을 사용할 수 있습니다. 이를 통해 새로운 도구나 상당한 교육 없이도 SQL을 아는 모든 사람(데이터 분석가, 개발자 및 비즈니스 인텔리전스 사용자)이 정교한 기계 학습 기술을 사용할 수 있습니다.

데이터는 머신 러닝에서 가장 중요한 단일 요소이며 데이터는 데이터베이스에 있습니다. 그렇다면 왜 다른 곳에서 기계 학습을 수행합니까?

![Machine Learning in Database using SQL](/assets/mdb_image.png)

## 비젼:

***사람들이 지능형 데이터베이스를 사용하여 더 나은 데이터 기반 의사 결정을 내리는 세상.***

...이러한 결정을 내리는 가장 좋은 방법은 새 데이터베이스를 만들 필요 없이 기존 데이터베이스 내에서 이 기능을 활성화하는 것입니다..

(이름에도 불구하고 우리는 실제로 데이터베이스가 아닙니다.!)


## 데이터베이스 테이블에서 기계 학습 모델로

!!! 정보 "빠른 예를 들어 SqFt 및 Price of Home Rentals를 저장하는 데이터베이스를 살펴보겠습니다.:"

    ```sql
    SELECT sqft, price FROM home_rentals_table;
    ```

    ![SQFT vs Price](/assets/info/sqft-price.png)

!!! 노트 "이 테이블의 정보에 대해 데이터베이스를 쿼리하고 검색 기준이 일치를 생성하는 경우 결과를 얻습니다.:"
    

    ```sql
    SELECT sqft, price FROM home_rentals_table WHERE sqft = 900;
    ```

    ![SELECT FROM Table](/assets/info/select.png)

!!! 노트 "검색 기준이 일치를 생성하지 않는 경우 - 결과가 비어 있습니다:"

    ```sql
    SELECT sqft, price FROM home_rentals_table WHERE sqft = 800;
    ```

    ![SELECT FROM Table No Results](/assets/info/selectm.png)

!!! 팁 "ML 모델은 주택 임대 테이블의 데이터에 적합할 수 있습니다.."

    ```sql
    CREATE PREDICTOR  home_rentals_model FROM home_rentals_table PREDICT price;   
    ```

    ![Model](/assets/info/model.png)

!!! 성공 "ML 모델은 소득 테이블에 정확히 일치하는 항목이 없는 검색에 대한 대략적인 답변을 제공할 수 있습니다.:"
    

    ```sql
    SELECT sqft, price FROM home_rentals_model WHERE sqft = 800;
    ```

    ![Query model](/assets/info/query.png)


## 시작

MindsDB 사용을 시작하려면 다음을 확인하십시오. [Getting Started Guide](/info)
