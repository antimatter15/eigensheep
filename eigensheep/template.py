import os, sys, ast, pprint, pickle, base64, zlib, hashlib

# TODO: consider using https://github.com/ipython/ipython/blob/
#                      master/IPython/core/interactiveshell.py
# Based on: https://stackoverflow.com/a/47130538

sys.path.append(os.path.join(os.path.dirname(__file__), 'python_lambda_deps'))


def lambda_handler(event, context):
    if event['type'] == 'RUN':
        return lambda_run(event, context)
    elif event['type'] == 'BUILD':
        return lambda_build(event, context)


def lambda_build(event, context):
    import subprocess
    import boto3
    import shutil

    s3 = boto3.resource('s3')
    
    os.chdir('/tmp')
    path = '/tmp/deps'
    if os.path.exists(path):
        shutil.rmtree(path)
    proc = subprocess.Popen([
            '/var/lang/bin/pip',
            'install',
            '--no-cache-dir',
            '--progress-bar=off',
            '--target=' + path
        ] + event['requirements'], 
        stdout=subprocess.PIPE, 
        stderr=subprocess.STDOUT)

    output = proc.communicate()[0]
    package = build_lambda_package(path)

    s3.Bucket(event['s3_bucket']).put_object(
            Key=event['s3_key'], 
            Body=package)

    return {
        'output': output.decode('utf-8')
    }


def zipdir(ziph, path, realpath):
    for root, dirs, files in os.walk(realpath):
        for file in files:
            ziph.write(os.path.join(root, file),
                os.path.normpath(os.path.join(path, os.path.relpath(root, realpath), file)))

def zipstr(ziph, path, contents):    
    import zipfile
    info = zipfile.ZipInfo(path)
    info.external_attr = 0o555 << 16 
    ziph.writestr(info, contents)


def build_lambda_package(dep_path):
    import io
    import zipfile

    pseudofile = io.BytesIO()
    zipf = zipfile.ZipFile(pseudofile, 'w', zipfile.ZIP_DEFLATED)
    
    zipdir(zipf, 'python_lambda_deps/', dep_path)
    zipstr(zipf, 'main.py', open("/var/task/main.py", "r").read())
    
    zipf.close()
    return pseudofile.getvalue()


def lambda_run(event, context):
    globalenv = {
        'INDEX': event['index'],
        'DATA': decode_result(event['data'])
    }
    if 'globals' in event:
        for key in event['globals']:
            globalenv[key] = event['globals'][key]
    result = my_exec(event['code'], globalenv, globalenv)

    output = {
        'machine': os.environ['AWS_LAMBDA_LOG_STREAM_NAME'],
        'pretty': pprint.pformat(result, indent=4),
        'result': encode_result(result, {
            's3_bucket': event['s3_bucket']
        })
    }

    return output



def encode_result(data, ctx = {}):
    # TODO: automatically choose the highest pickle version which is compatible

    data = base64.b64encode(zlib.compress(pickle.dumps(data, 2))).decode('utf-8')

    result = {
        "type": "b64+zlib+pickle",
        "data": data
    }

    if len(data) > 5 * 1024 * 1024 and ctx.has_key("s3_bucket"):
        import zipfile
        import json
        import boto3
        s3 = boto3.resource('s3')

        contents = json.dumps(result)
        hashed = hashlib.md5(contents).hexdigest()
        s3_key = 'chunks/' + hashed

        s3.Bucket(ctx['s3_bucket']).put_object(
            Key=s3_key, 
            Body=contents)

        result = {
            "type": "s3",
            "s3_key": s3_key,
            "s3_bucket": ctx['s3_bucket']
        }

    return result


def decode_result(data):
    if data['type'] == 's3':
        import io
        import boto3
        s3 = boto3.resource('s3')
        pseudofile = io.BytesIO()
        s3.Bucket(data['s3_bucket']).download_fileobj(data['s3_key'], pseudofile)

        return decode_result(json.load(pseudofile))
    elif data['type'] == 'b64+zlib+pickle':
        return pickle.loads(zlib.decompress(base64.b64decode(data['data'])))

        
        
def my_exec(script, globals=None, locals=None):
    '''Execute a script and return the value of the last expression'''
    stmts = list(ast.iter_child_nodes(ast.parse(script)))
    if not stmts:
        return None
    if isinstance(stmts[-1], ast.Expr):
        # the last one is an expression and we will try to return the results
        # so we first execute the previous statements
        if len(stmts) > 1:
            exec(compile(ast.Module(body=stmts[:-1]), 
                filename="<ast>", mode="exec"), globals, locals)
        # then we eval the last one
        return eval(compile(ast.Expression(body=stmts[-1].value), 
            filename="<ast>", mode="eval"), globals, locals)
    else:
        # otherwise we just execute the entire code
        exec(script, globals, locals)