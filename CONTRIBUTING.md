# Publishing

```
# reformat code with black
black eigensheep

# upload current cloudformation template to s3 bucket
aws s3 cp cloudformation/template.yaml s3://eigensheep/template.yaml --acl public-read

# upload to pypi
trash dist
python3 setup.py sdist bdist_wheel
twine upload dist/*
```