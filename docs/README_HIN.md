# MindsDB प्रलेखन

## डॉक्स स्थानीय रूप से चल

सबसे पहले अपने अजगर आभासी वातावरण में mkdocs और mkdocs- सामग्री विषय स्थापित करें:
```
pip install -r requirements.txt
```
फिर, `/mindsdb-docs` निर्देशिका में नेविगेट करें और सर्वर प्रारंभ करें:

```
mkdocs serve
```

प्रलेखन वेबसाइट `http://127.0.0.1:8000` पर उपलब्ध होगी 


## Deploy the docs

नवीनतम संस्करण को मास्टर पर मर्ज करने के बाद स्वचालित रूप से धक्का दिया जाएगा और तैनात किया जाएगा. यदि CI/CD परिनियोजन विफल रहा, स्थानीय रूप से भागो:

```
mkdocs gh-deploy
```

सभी एचटीएमएल फाइलों और संपत्तियों को यहां धकेल दिया जाएगा [gh-pages](https://github.com/mindsdb/mindsdb-docs/tree/gh-pages) शाखा और जीथब पृष्ठों पर प्रकाशित.

## भंडार संरचना

mindsdb-docs लेआउट इस प्रकार है:

```
docs                                   # दस्तावेज़ स्रोत फ़ाइलें शामिल हैं
|__assets/                             # पृष्ठों में प्रयुक्त छवि और चिह्न
|__.md                                 # पेज के रूप में उपयोग की जाने वाली सभी मार्कडाउन फाइलें
overrides
├─ assets/
│  ├─ images/                          # छवियाँ और चिह्न
│  ├─ javascripts/                     # JavaScript
│  └─ stylesheets/                     # Stylesheets
├─ partials/
│  ├─ footer.html                      # Footer bar
├─ 404.html                            # 404 त्रुटि पृष्ठ
├─ base.html                           # आधार टेम्पलेट
└─ main.html
.mkdocs.yml                            # Mkdocs विन्यास फाइल
```
# योगदान

## आप हमारी कैसे मदद कर सकते हैं? [![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)](https://github.com/mindsdb/mindsdb/issues)

* गलती सूचित करें
* दस्तावेज़ीकरण में सुधार करें
* नई सुविधा का प्रस्ताव
* टाइपो को ठीक करें

## Contributors

<a href="https://github.com/mindsdb/mindsdb-docs/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=mindsdb/mindsdb-docs" />
</a>

बना हुआ [contributors-img](https://contributors-img.web.app).
