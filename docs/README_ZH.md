# MindsDB 文档

## 在本地运行

首先在python虚拟环境中安装mkdocs和mkdocs-material主题:
```
pip install -r requirements.txt
```
然后，进入`/mindsdb-docs`目录并启动服务器:

```
mkdocs serve
```

文档的网站将在`http://127.0.0.1:8000`


## 部署文档

在master上合并后，最新版本将自动推送和部署。如果CI/CD部署失败，则在本地运行:

```
mkdocs gh-deploy
```

所有的html文件和附件将被推到[gh-pages](https://github.com/mindsdb/mindsdb-docs/tree/gh-pages)分支和发布在github页面。

## 库结构

mindsdb-docs布局如下所示:

```
docs                                   # 包含文档源文件
|__assets/                             # 在页面中使用的图像和图标
|__.md                                 # 用作页面的所有markdown文件
overrides
├─ assets/
│  ├─ images/                          # 图片和图标
│  ├─ javascripts/                     # JavaScript
│  └─ stylesheets/                     # Stylesheets
├─ partials/
│  ├─ footer.html                      # Footer bar
├─ 404.html                            # 404错误也没
├─ base.html                           # 框架模板
└─ main.html
.mkdocs.yml                            # Mkdocs配置文件
```
# 贡献

## 你如何帮助我们?[![欢迎贡献](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)](https://github.com/mindsdb/mindsdb/issues)

* 报告bug
* 提交文档
* 建议新功能
* 修正拼写错误

## 贡献者

<a href="https://github.com/mindsdb/mindsdb-docs/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=mindsdb/mindsdb-docs" />
</a>

使用 [contributors-img](https://contributors-img.web.app).
