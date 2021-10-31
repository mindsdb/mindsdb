# MindsDB 문서


## 로컬에서 문서 실행


먼저 파이썬 가상 환경에 mkdocs 및 mkdocs-material 테마를 설치하십시오:
```
pip install -r requirements.txt
```
그런 다음 `/mindsdb-docs` 디렉토리로 이동하여 서버를 시작합니다:

```
mkdocs serve
```

문서 웹 사이트는 `http://127.0.0.1:8000`에서 사용할 수 있습니다.


## 문서 배포


최신 버전은 마스터에 병합 후 자동으로 푸시 및 배포됩니다. CI/CD 배포에 실패한 경우 로컬에서 다음을 실행합니다:
```
mkdocs gh-deploy
```

모든 html 파일과 자산은 [gh-pages](https://github.com/mindsdb/mindsdb-docs/tree/gh-pages) 분기로 푸시되고 github 페이지에 게시됩니다.


## 저장소 구조 

mindsdb-docs 레이아웃은 다음과 같습니다:

```
docs                                   # Contains documentation source files
|__assets/                             # Image and icons used in pages
|__.md                                 # All of the markdown files used as pages
overrides
├─ assets/
│  ├─ images/                          # Images and icons
│  ├─ javascripts/                     # JavaScript
│  └─ stylesheets/                     # Stylesheets
├─ partials/
│  ├─ footer.html                      # Footer bar
├─ 404.html                            # 404 error page
├─ base.html                           # Base template
└─ main.html
.mkdocs.yml                            # Mkdocs configuration file
```
# 기여 

## 어떻게 도와드릴까요? [![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)](https://github.com/mindsdb/mindsdb/issues)

* Report a bug
* Improve documentation
* Propose new feature
* Fix typos

## 기여자들 

<a href="https://github.com/mindsdb/mindsdb-docs/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=mindsdb/mindsdb-docs" />
</a>

Made with [contributors-img](https://contributors-img.web.app).
