rm -rf build/*
rm -rf dist/*
rm -rf mindsdb.egg-info/*
python setup.py develop --uninstall
python setup.py develop
python3 setup.py sdist bdist_wheel

echo "Do you want to publish this version (yes/no)?"

read publish

if [ "$publish" = "yes" ]; then
    echo "Publishing mindsdb to Pypi"
    python3 -m twine upload dist/*
fi


