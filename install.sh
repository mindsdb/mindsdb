rm -rf build/*
rm -rf dist/*
rm -rf mindsdb.egg-info/*
python setup.py develop --uninstall
python setup.py develop
python3 setup.py sdist bdist_wheel

