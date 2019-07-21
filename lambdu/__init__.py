from __future__ import print_function

from IPython.core.display import display, HTML
from IPython.core.magic import register_cell_magic
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm_notebook as tqdm
import boto3

import distutils.spawn, subprocess, threading, argparse, tempfile, inspect, hashlib, \
    zipfile, pickle, base64, shutil, json, zlib, sys, os, io


AWS_PROFILE = 'default'
FUNCTION_NAME = 'parallel_lambda'
LAMBDA_ROLE = 'arn:aws:iam::972882471061:role/lambda_exec_role'
DEFAULT_MEMORY = 128
DEFAULT_TIMEOUT = 30
MAX_CONCURRENCY = 1000

# the python package name. this is not configuration.
name = "lambdu"

threadLocal = threading.local()
executor = None
storedLambdas = {}

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

# if not distutils.spawn.find_executable("docker"):
#     # unable to find executable for docker
#     eprint("Warning: Could not find `docker` executable. Lambdu will not be able to execute cells with pip dependencies.")
# else:

#     child = subprocess.Popen(['docker', 'ps'], stderr=subprocess.PIPE)
#     streamdata = child.communicate()[1]
#     # if there was some error in running `docker ps`
#     if child.returncode != 0:
#         if 'permission denied' in str(streamdata):
#             eprint("Warning: Permission denied to invoke `docker`. " + \
#                 "Consider following this tutorial to allow docker to be managed by non-root users: " + \
#                 "https://docs.docker.com/install/linux/linux-postinstall/#manage-docker-as-a-non-root-user\n")
#         else:
#             eprint("Warning: An error occurred when running `docker ps`. Lambdu will not be able to execute cells with pip dependencies.\n")
#         eprint(streamdata.decode('utf-8').strip())

display(HTML('Prefix cells with <code>%%lambdu [-n CONCURRENCY] [dependencies...]</code> to run in AWS Lambda. <a target="_blank" href="https://github.com/antimatter15/lambdu">Learn more...</a>'))


def ensure_setup():
    global executor
    if executor is None:
        executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENCY)
    if not hasattr(threadLocal, 'client'):
        session = boto3.session.Session(profile_name=AWS_PROFILE)
        threadLocal.lambdaClient = session.client('lambda')


@register_cell_magic
def lambdu(line, cell):
    default_lambda_runtime = 'python3.6'
    if sys.version_info[0] == 2:
        default_lambda_runtime = 'python2.7'

    parser = argparse.ArgumentParser(
        prog='lambdu', 
        description='Jupyter cell magic to invoke cell on AWS Lambda')

    parser.add_argument('deps', type=str, nargs='*',
                        help='dependencies to be installed via PyPI')

    parser.add_argument('--memory', default=DEFAULT_MEMORY, type=int,
                        help='amount of memory in 64MB increments from 128 up to 3008')

    parser.add_argument('--timeout', default=DEFAULT_TIMEOUT, type=int,
                        help='lambda execution timeout in seconds up to 900 (15 minutes)')

    parser.add_argument('--no_install', action='store_true',
                        help='do not install dependencies if not found')

    parser.add_argument('--clean_all', action='store_true',
                        help='remove all deployed dependencies')

    parser.add_argument('--rm', action='store_true',
                        help='remove a specific')

    parser.add_argument('--reinstall', action='store_true',
                        help='uninstall and reinstall')

    parser.add_argument('--runtime', type=str, default=default_lambda_runtime,
                        help='which runtime (python3.6, python2.7)')

    parser.add_argument('-n', type=int, default=1,
                        help='number of lambdas to invoke')

    parser.add_argument('--verbose', action='store_true',
                        help='show additional information from lambda invocation')

    parser.add_argument('--name', type=str,
                        help='name to store this lambda as')

    args = parser.parse_args(line.split(' '))
    deps = [ x for x in args.deps if x ]

    box_config = {
        'requirements': deps,
        'memory': args.memory,
        'timeout': args.timeout,
        'runtime': args.runtime
    }

    alias = make_alias_name(box_config)

    if args.clean_all:
        remove_all_aliases()
        return

    if args.rm or args.reinstall:
        ensure_setup()
        try:
            ali = threadLocal.lambdaClient.get_alias(FunctionName=FUNCTION_NAME, Name=alias)
            threadLocal.lambdaClient.delete_alias(FunctionName=FUNCTION_NAME, Name=ali['Name'])
            threadLocal.lambdaClient.delete_function(FunctionName=FUNCTION_NAME, 
                Qualifier=ali['FunctionVersion'])
            eprint('Deleted alias "%s".' % alias)
        except threadLocal.lambdaClient.exceptions.ResourceNotFoundException:
            pass
        
        if args.rm:
            return

    if not args.no_install and not lambda_exists(FUNCTION_NAME, alias):
        ensure_deps(box_config)
        # eprint(("-" * 100) +  "\n")


    run_config = {
        'box': box_config,
        'alias': alias,
        'code': cell,
        'verbose': args.verbose
    }

    if args.name:
        storedLambdas[args.name] = run_config
        eprint('Invoke this stored cell with `lambdu.invoke("%s")` or `lambdu.map("%s", [1, 2, ...])`' % (args.name, args.name))
        return None

    if args.n > 1:
        return map(run_config, range(args.n))
    else:
        return invoke(run_config)


def install_lambda_deps(path, box_config):
    requirements = box_config['requirements']
    runtime = box_config['runtime']

    if len(requirements) == 0:
        return

    if 'python' in box_config['runtime']:
        proc = subprocess.Popen(
            ['docker', 'run', '-t', '--rm', 
             '-v', path + ':' + '/var/task', 
             '--entrypoint', '/var/lang/bin/pip', 'lambci/lambda:build-' + runtime, 
             'install', '--progress-bar=off',  '-t', '/var/task'] + requirements, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE)
    elif 'nodejs' in box_config['runtime']:
        proc = subprocess.Popen(
            ['docker', 'run', '-t', '--rm', 
             '-v', path + ':' + '/var/task', 
             '--entrypoint', '/var/lang/bin/npm', 'lambci/lambda:build-' + runtime, 
             'install', '--cache=/tmp/.npm'] + requirements, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT)

    for line in proc.stdout:
        line = line.decode('utf-8')
        if 'Downloading ' not in line and line.strip() != '':
            eprint(line.strip())

    # for line in proc.stderr:
    #     eprint(line, end='')

    if child.returncode != 0:
        raise Exception("Unable to install dependencies. Make sure that Docker is installed, " + \
            "running, and the current user has permissions to manage the Docker daemon.")


# TODO: consider using https://github.com/ipython/ipython/blob/
#                      master/IPython/core/interactiveshell.py
# Based on: https://stackoverflow.com/a/47130538

LAMBDA_TEMPLATE_PYTHON = """
import os, sys, ast, pprint, pickle, base64, zlib

sys.path.append(os.path.join(os.path.dirname(__file__), 'python_lambda_deps'))

def pickle_serialize(out):
    try:
        data = base64.b64encode(zlib.compress(pickle.dumps(out, 2))).decode('utf-8')
        if len(data) > 5 * 1024 * 1024:
            # Don't use print() because this code needs to run on both py2k and py3k
            sys.stdout.write('WARN/LAMBDU: Pickle serialization exceeds 5MB, not returning result.\\n')
            return (False, None)
        return (True, data)
    except Exception:
        return (False, None)

def lambda_handler(event, context):
    globalenv = {
        'INDEX': event['index'],
        'DATA': pickle.loads(zlib.decompress(base64.b64decode(event['zpickle64'])))
    }
    result = my_exec(event['code'], globalenv, globalenv)

    output = {
        'machine': os.environ['AWS_LAMBDA_LOG_STREAM_NAME'],
        'pretty': pprint.pformat(result, indent=4),
    }

    pickle_serializable, pickle_data = pickle_serialize(result)
    if pickle_serializable:
        output['zpickle64'] = pickle_data

    return output

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
"""

LAMBDA_TEMPLATE_NODEJS = """
module.exports.lambda_handler = function(event, context, callback){
    var result = eval(event.code);

    var output = {
        'machine': process.env['AWS_LAMBDA_LOG_STREAM_NAME'],
        'pretty': require('util').inspect(result)
    }
    try {
        var s = JSON.stringify(result)
        if(s.length < 5 * 1024 * 1024){
            output['json'] = result;
        }
    } catch (err) { }
    callback(null, output);
}
"""

def zipdir(ziph, path, realpath):
    for root, dirs, files in os.walk(realpath):
        for file in files:
            ziph.write(os.path.join(root, file),
                os.path.normpath(os.path.join(path, os.path.relpath(root, realpath), file)))

def zipstr(ziph, path, contents):    
    info = zipfile.ZipInfo(path)
    info.external_attr = 0o555 << 16 
    ziph.writestr(info, contents)

def build_lambda_package(dep_path, box_config):
    pseudofile = io.BytesIO()
    zipf = zipfile.ZipFile(pseudofile, 'w', zipfile.ZIP_DEFLATED)
    
    if 'python' in box_config['runtime']:
        zipdir(zipf, 'python_lambda_deps/', dep_path)
        zipstr(zipf, 'main.py', LAMBDA_TEMPLATE_PYTHON)
    
    elif 'nodejs' in box_config['runtime']:
        zipdir(zipf, '', dep_path)
        zipstr(zipf, 'main.js', LAMBDA_TEMPLATE_NODEJS)

    zipf.close()
    return pseudofile.getvalue()

def create_or_update_alias(version, alias):
    ensure_setup()
    try:
        return threadLocal.lambdaClient.update_alias(
            FunctionName=FUNCTION_NAME, Name=alias, FunctionVersion=version)
    except threadLocal.lambdaClient.exceptions.ResourceNotFoundException:    
        return threadLocal.lambdaClient.create_alias(
            FunctionName=FUNCTION_NAME, Name=alias, FunctionVersion=version)
    

def create_or_update_lambda(zipfile, box_config):
    ensure_setup()
    runtime = box_config['runtime']
    memory = box_config['memory']
    timeout = box_config['timeout']
    handler = 'main.lambda_handler'

    try:
        threadLocal.lambdaClient.update_function_configuration(
            FunctionName=FUNCTION_NAME,
            Timeout=timeout,
            Runtime=runtime,
            MemorySize=memory
        )
        return threadLocal.lambdaClient.update_function_code(
            FunctionName=FUNCTION_NAME,
            ZipFile=zipfile,
            Publish=True
        )
    except threadLocal.lambdaClient.exceptions.ResourceNotFoundException:
        return threadLocal.lambdaClient.create_function(
            FunctionName=FUNCTION_NAME, 
            Runtime=runtime,
            MemorySize=memory,
            Timeout=timeout,
            Code={
                'ZipFile': zipfile
            },
            Handler=handler,
            Publish=True,
            Role=LAMBDA_ROLE,
            Description='Lambdu Parallel Lambda Worker'
        )


def human_size(bytes, units=[' bytes','KB','MB','GB','TB', 'PB', 'EB']):
    """ Returns a human readable string reprentation of bytes"""
    return str(bytes) + units[0] if bytes < 1024 else human_size(bytes>>10, units[1:])


def ensure_deps(box_config):
    alias = make_alias_name(box_config)
    if lambda_exists(FUNCTION_NAME, alias):
        eprint("Alias '%s' already exists." % alias)
        return
    # path = tempfile.mkdtemp('python_lambda_deps')
    # os.chmod(path, 0o777)
    path = os.path.join(os.getcwd(), 'temp_lambda_deps')
    if os.path.exists(path):
        shutil.rmtree(path)
    eprint("Installing %d dependencies..." % len(box_config['requirements']))
    install_lambda_deps(path, box_config)
    eprint("Building Lambda package...")
    package_contents = build_lambda_package(path, box_config)
    if os.path.exists(path):
        shutil.rmtree(path)
    eprint("Uploading package to AWS (%s)..." % human_size(len(package_contents)))
    result = create_or_update_lambda(package_contents, box_config)
    create_or_update_alias(result['Version'], alias)
    eprint("Successfully deployed as '%s'." % alias)


def remove_all_aliases():
    ensure_setup()
    aliases = threadLocal.lambdaClient.list_aliases(FunctionName=FUNCTION_NAME)['Aliases']
    versions = threadLocal.lambdaClient.list_versions_by_function(
        FunctionName=FUNCTION_NAME)['Versions']

    for ali in aliases:
        threadLocal.lambdaClient.delete_alias(FunctionName=FUNCTION_NAME, Name=ali['Name'])

    for ver in versions:
        if ver['Version'] == '$LATEST': continue
        threadLocal.lambdaClient.delete_function(FunctionName=FUNCTION_NAME, Qualifier=ver['Version'])

    eprint("Removed %d aliases, and %d versions" % (len(aliases), len(versions) - 1))



def make_alias_name(box_config):
    requirements = sorted([x.lower() for x in set(box_config['requirements'])])
    h = hashlib.sha256(b'1')
    for req in requirements: h.update(req.encode('utf-8'))
    reqs = '_'.join(requirements)[:100]
    if reqs == '': reqs = 'NO-DEPS'
    return ('%s-%dM-%ds-%s-' % (
        box_config['runtime'].replace('.', ''), 
        box_config['memory'], 
        box_config['timeout'], 
        h.hexdigest()[:5])) + reqs    


def lambda_exists(name, alias):
    ensure_setup()
    try:
        threadLocal.lambdaClient.invoke(FunctionName=name, InvocationType="DryRun", Qualifier=alias)
    except threadLocal.lambdaClient.exceptions.ResourceNotFoundException:
        return False
    return True


def invoke_thread(info):
    ensure_setup()
    result = threadLocal.lambdaClient.invoke(
        FunctionName=FUNCTION_NAME,
        InvocationType='RequestResponse',
        LogType='Tail',
        Payload=info['payload'],
        Qualifier=info['alias']
    )
    data = json.load(result['Payload'])
    for line in base64.b64decode(result['LogResult']).decode('utf-8').split('\n')[:-1]:
        is_aws = line.startswith('START ') or line.startswith('END ') or line.startswith('REPORT ')
        if (not is_aws) or (info['verbose']):
            print(line)

    if data is not None:
        if 'zpickle64' in data:
            return pickle.loads(zlib.decompress(base64.b64decode(data['zpickle64'])))
        elif 'json' in data:
            return data['json']
        elif 'pretty' in data:
            return data['pretty']
        else:
            return data

def map(run_config, data = [0]):
    ensure_setup()
    
    if isinstance(run_config, str):
        run_config = storedLambdas[run_config]

    count = len(data)
    tasks = []
    box_config = run_config['box']

    for i, data in enumerate(data):
        payload = {
            'code': run_config['code'],
            'index': i
        }

        if 'python' in box_config['runtime']:
            # TODO: automatically choose the highest pickle version which is compatible
            payload['zpickle64'] = base64.b64encode(zlib.compress(pickle.dumps(data, 2))).decode('utf-8')
        else:
            payload['json'] = data

        tasks.append({
            'alias': run_config['alias'],
            'verbose': run_config.get('verbose', False),
            'payload': json.dumps(payload)
        })

    if count == 1:
        return [invoke_thread(tasks[0])]
    else:
        return list(tqdm(executor.map(invoke_thread, tasks), total=count))


def invoke(run_config, data = 0):
    return map(run_config, [data])[0]

