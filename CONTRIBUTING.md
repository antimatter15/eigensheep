# Publishing

```
aws s3 cp cloudformation/template.yaml s3://eigensheep/template.yaml --acl public-read

trash dist
python3 setup.py sdist bdist_wheel
twine upload dist/*
```