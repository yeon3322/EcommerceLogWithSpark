# EcommerceLogWithSpark
Ecommerce에서 생성된 행동 로그를 Spark Java App을 통해 Hive 외부 테이블로 제공하는 프로젝트입니다.

## Version and Prior preparation
* JDK 1.8.0   
* Maven 3.8.1
* Spark 3.4.4
* Hive & Hadoop HDFS

## Usage
**1. 사용 데이터 다운로드 후 프로젝트 하위 저장**
* [데이터 링크](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store?select=2019-Oct.csv)
* 프로젝트 하위에 `data` 디렉토리 생성
* `data` 디렉토리 하위에 csv 파일 저장
    
**3. MAVEN 빌드**
```
mvn clean install compile
```

**2. 생성된 Jar 파일 실행 (Spark bin 위치에서 실행)**
```
./bin/spark-submit --class org.example.Main --master local EcommerceLogWithSpark-1.0-SNAPSHOT.jar 2019-Oct.csv
```
