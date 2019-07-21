from IPython.core.magic import Magics, magics_class, line_cell_magic
from IPython.core.display import display, HTML, Javascript
from ipywidgets import widgets
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm_notebook as tqdm
from IPython.core.error import UsageError
from os.path import expanduser
from types import ModuleType
import configparser
import hashlib
import threading
import boto3
import argparse
import os
import io
import sys
import zipfile
import base64
import zlib
import time
import pickle
import json
import ast
import re

AWS_PROFILE = 'eigensheep'
FUNCTION_NAME = 'EigensheepLambda'
BUCKET_PREFIX = 'eigensheep-'
STACK_TEMPLATE_URL = 'https://eigensheep.s3.amazonaws.com/template.yaml'
GITHUB_URL = 'https://github.com/antimatter15/lambdu'


DEFAULT_MEMORY = 512
DEFAULT_TIMEOUT = 60
MAX_CONCURRENCY = 1000


BOOTSTRAP_CONFIG = {
    'requirements': [],
    'memory': 512,
    'timeout': 300,
    'runtime': 'python3.6'
}

threadLocal = threading.local()
executor = None
storedLambdas = {}
accountID = None
known_aliases = set([])


parser = argparse.ArgumentParser(
    prog='eigensheep', 
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

parser.add_argument('--runtime', type=str, default='python3.6',
                    help='which runtime (python3.6, python2.7)')

parser.add_argument('-n', type=int, default=1,
                    help='number of lambdas to invoke')

parser.add_argument('--verbose', action='store_true',
                    help='show additional information from lambda invocation')

parser.add_argument('--name', type=str,
                    help='name to store this lambda as')

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def bucketID():
    global accountID
    return BUCKET_PREFIX + accountID


def ensure_setup():
    global executor, accountID, known_aliases
    if executor is None:
        executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENCY)

    # if we have already defined the lambda client skip the rest
    if hasattr(threadLocal, 'lambdaClient'):
        return

    session = boto3.session.Session(profile_name=AWS_PROFILE)
    threadLocal.lambdaClient = session.client('lambda')
    threadLocal.s3Client = session.client('s3')

    # if we have already loaded the accountID then skip the rest
    if accountID:
        return

    accountID = session.client('sts').get_caller_identity().get('Account')

    # load all the known aliases
    aliases = threadLocal.lambdaClient.list_aliases(FunctionName=FUNCTION_NAME)['Aliases']
    known_aliases = set([ ali['Name'] for ali in aliases ])

    # check that the appropriate bucket exists
    threadLocal.s3Client.head_bucket(Bucket=bucketID())

    # check that the lambda function exists
    if not lambda_exists(FUNCTION_NAME, None):
        raise Exception("No lambda exists with name '%s'."  % FUNCTION_NAME)

def lambda_exists(name, alias):
    ensure_setup()
    global known_aliases
    try:
        if alias:
            threadLocal.lambdaClient.invoke(FunctionName=name, InvocationType="DryRun", Qualifier=alias)
        else:
            threadLocal.lambdaClient.invoke(FunctionName=name, InvocationType="DryRun")
    except threadLocal.lambdaClient.exceptions.ResourceNotFoundException:
        if alias in known_aliases:
            known_aliases.remove(alias)
        return False
    known_aliases.add(alias)
    return True

def hide_traceback(exc_tuple=None, filename=None, tb_offset=None,
                   exception_only=False, running_compiled_code=False):
    
    etype, value, tb = sys.exc_info()

    if issubclass(etype, QuietError):
        value = value.error
        etype = type(value)
        return ipython._showtraceback(etype, value, ipython.InteractiveTB.get_exception_only(etype, value))

    return ipython.original_showtraceback(
        exc_tuple=exc_tuple, 
        filename=filename, 
        tb_offset=tb_offset, 
        exception_only=exception_only, 
        running_compiled_code=running_compiled_code)



class QuietError(Exception):
    def __init__(self, error):
        # super(Exception).__init__(str(error))
        super().__init__(str(error))
        self.error = error


def show_setup(setup_error):
    access_key = widgets.Text(
        description="Access Key: ",
        placeholder="AKIAJXSDOIF")

    secret_key = widgets.Text(
        description="Secret Key: ",
        placeholder="1/Wi3ns8e3nKLSeiwnMn")

    region = widgets.Text(
        description="Region: ",
        placeholder="us-east-1",
        value="us-east-1")

    # display cloudformation button
    display(HTML("""
    <img src="https://raw.githubusercontent.com/antimatter15/lambdu/master/logo.png" style="width: 300px; max-width: 100%"/>
    <b>It looks like you haven't set up Eigensheep yet</b>. 

    You can get started with Eigensheep with just a few clicks by following these instructions:<br/>
    <ol>
        <li>Automatically generate Eigensheep resources using AWS CloudFormation with this button: <br/><a target="_blank" href="https://console.aws.amazon.com/cloudformation/home#/stacks/new?stackName=eigensheep&amp;templateURL=""" + STACK_TEMPLATE_URL + """">
            <svg width="144" height="27" viewBox="0 0 144 27" xmlns="http://www.w3.org/2000/svg">
                <title>Launch Stack</title>
                <defs>
                    <linearGradient x1="50%" y1="0%" x2="50%" y2="100%" id="a">
                        <stop stop-color="#FFE4B2" offset="0%" />
                        <stop stop-color="#F79800" offset="100%" />
                    </linearGradient>
                    <linearGradient x1="45.017%" y1="100%" x2="68.082%" y2="3.32%" id="b">
                        <stop stop-color="#151443" offset="0%" />
                        <stop stop-color="#6D80B2" offset="100%" />
                    </linearGradient>
                </defs>
                <g fill="none" fill-rule="evenodd">
                    <path d="M2 5v17c0 1.66 1.34 3 3 3h125.5c6.348 0 11.5-5.15 11.5-11.5C142 7.148 136.852 2 130.5 2H5C3.34 2 2 3.34 2 5z" fill="url(#a)" />
                    <path d="M2 5v17c0 1.66 1.34 3 3 3h125.5c6.348 0 11.5-5.15 11.5-11.5C142 7.148 136.852 2 130.5 2H5C3.34 2 2 3.34 2 5zM0 5c0-2.762 2.233-5 5-5h125.5c7.456 0 13.5 6.043 13.5 13.5 0 7.456-6.05 13.5-13.5 13.5H5c-2.762 0-5-2.232-5-5V5z" fill="#0058A5" />
                    <circle fill="url(#b)" cx="129.5" cy="13.5" r="9.5" />
                    <path fill="#FFF" d="M133 13.5l-5 4.5V9" />
                    <path d="M18.136 19h6.4v-1.648h-4.432V8.216h-1.968V19zm14.068 0c-.08-.48-.112-1.168-.112-1.872v-2.816c0-1.696-.72-3.28-3.216-3.28-1.232 0-2.24.336-2.816.688l.384 1.28c.528-.336 1.328-.576 2.096-.576 1.376 0 1.584.848 1.584 1.36v.128c-2.88-.016-4.624.976-4.624 2.944 0 1.184.88 2.32 2.448 2.32 1.008 0 1.824-.432 2.304-1.04h.048l.128.864h1.776zm-2.032-2.736c0 .128-.016.288-.064.432-.176.56-.752 1.072-1.536 1.072-.624 0-1.12-.352-1.12-1.12 0-1.184 1.328-1.488 2.72-1.456v1.072zm11.076-5.056H39.28v4.704c0 .224-.048.432-.112.608-.208.496-.72 1.056-1.504 1.056-1.04 0-1.456-.832-1.456-2.128v-4.24H34.24v4.576c0 2.544 1.296 3.392 2.72 3.392 1.392 0 2.16-.8 2.496-1.36h.032L39.584 19h1.728c-.032-.64-.064-1.408-.064-2.336v-5.456zM43.476 19h1.984v-4.576c0-.224.016-.464.08-.64.208-.592.752-1.152 1.536-1.152 1.072 0 1.488.848 1.488 1.968V19h1.968v-4.624c0-2.464-1.408-3.344-2.768-3.344-1.296 0-2.144.736-2.48 1.344h-.048l-.096-1.168h-1.728c.048.672.064 1.424.064 2.32V19zm14.708-1.696c-.384.16-.864.304-1.552.304-1.344 0-2.384-.912-2.384-2.512-.016-1.424.88-2.528 2.384-2.528.704 0 1.168.16 1.488.304l.352-1.472c-.448-.208-1.184-.368-1.904-.368-2.736 0-4.336 1.824-4.336 4.16 0 2.416 1.584 3.968 4.016 3.968.976 0 1.792-.208 2.208-.4l-.272-1.456zM60.012 19h1.984v-4.656c0-.224.016-.432.08-.592.208-.592.752-1.104 1.52-1.104 1.088 0 1.504.848 1.504 1.984V19h1.968v-4.592c0-2.496-1.392-3.376-2.72-3.376-.496 0-.96.128-1.344.352-.416.224-.736.528-.976.896h-.032V7.64h-1.984V19zm12.264-.512c.592.352 1.776.672 2.912.672 2.784 0 4.096-1.504 4.096-3.232 0-1.552-.912-2.496-2.784-3.2-1.44-.56-2.064-.944-2.064-1.776 0-.624.544-1.296 1.792-1.296 1.008 0 1.76.304 2.144.512l.48-1.584c-.56-.288-1.424-.544-2.592-.544-2.336 0-3.808 1.344-3.808 3.104 0 1.552 1.136 2.496 2.912 3.136 1.376.496 1.92.976 1.92 1.792 0 .88-.704 1.472-1.968 1.472-1.008 0-1.968-.32-2.608-.688l-.432 1.632zm9.076-9.04v1.76h-1.12v1.472h1.12v3.664c0 1.024.192 1.728.608 2.176.368.4.976.64 1.696.64.624 0 1.136-.08 1.424-.192l-.032-1.504c-.176.048-.432.096-.768.096-.752 0-1.008-.496-1.008-1.44v-3.44h1.872v-1.472h-1.872V8.984l-1.92.464zM92.892 19c-.08-.48-.112-1.168-.112-1.872v-2.816c0-1.696-.72-3.28-3.216-3.28-1.232 0-2.24.336-2.816.688l.384 1.28c.528-.336 1.328-.576 2.096-.576 1.376 0 1.584.848 1.584 1.36v.128c-2.88-.016-4.624.976-4.624 2.944 0 1.184.88 2.32 2.448 2.32 1.008 0 1.824-.432 2.304-1.04h.048l.128.864h1.776zm-2.032-2.736c0 .128-.016.288-.064.432-.176.56-.752 1.072-1.536 1.072-.624 0-1.12-.352-1.12-1.12 0-1.184 1.328-1.488 2.72-1.456v1.072zm9.556 1.04c-.384.16-.864.304-1.552.304-1.344 0-2.384-.912-2.384-2.512-.016-1.424.88-2.528 2.384-2.528.704 0 1.168.16 1.488.304l.352-1.472c-.448-.208-1.184-.368-1.904-.368-2.736 0-4.336 1.824-4.336 4.16 0 2.416 1.584 3.968 4.016 3.968.976 0 1.792-.208 2.208-.4l-.272-1.456zm3.796-9.664h-1.968V19h1.968v-2.656l.672-.784 2.24 3.44h2.416l-3.296-4.608 2.88-3.184h-2.368l-1.888 2.512c-.208.272-.432.608-.624.912h-.032V7.64z" fill="#000" />
                </g>
            </svg>
        </a>
            <br />
            <i style="color: #666">This stack creates an S3 bucket, Lambda function, execution role for the Lambda, and an IAM user with access limited to the S3 bucket and Lambda function. Eigensheep Lambdas have no access to any of your AWS resources besides its designated S3 bucket. You can verify the behavior of the stack by clicking on "View in Designer" at the linked wizard.</i>
        </li>
        <li>Click through the prompts accepting the default values for the Eigensheep stack.
            <br />
            <i style="color: #666">
            Make sure to check the box acknowledging that the Eigensheep stack will create a limited permission user and lambda role.
            </i></li>
        <li>You should now be on the CloudFormation stack details screen for your new Eigensheep stack. Click the 'Outputs' tab. Press the refresh button to the right of the 'Outputs (0)' message every 30 seconds until the outputs appear.
            <br />
            <i style="color: #666">
                This process generally takes 1-2 minutes to complete. You could use this time to take a break from your computer and stretch. 
            </i>
        </li>
        <li>
            Copy the outputs into the form below to finish setting up:
            <br />
            <i style="color: #666">
            This will save your Eigensheep credentials to a profile named "eigensheep" in your ~/.aws/config file.
            </i>
        </li>
    </ol>
"""))

    button = widgets.Button(description="Submit")

    display(access_key)
    display(secret_key)
    display(region)
    display(button)

    def handle_submit(sender):
        config = configparser.ConfigParser()
        try:
            os.mkdir(expanduser("~/.aws"))
        except FileExistsError:
            pass

        config = configparser.ConfigParser()
        config.read(expanduser("~/.aws/config"))
        config['profile eigensheep'] = {
            'region': region.value,
            'aws_access_key_id': access_key.value,
            'aws_secret_access_key': secret_key.value
        }
        with open(expanduser("~/.aws/config"), 'w') as configfile:
            config.write(configfile)

        # based on https://stackoverflow.com/a/39639111
        # re-execute the cell
        display(Javascript("""
            var output_area = this;
            var cell_element = output_area.element.parents('.cell');
            var cell_idx = Jupyter.notebook.get_cell_elements().index(cell_element);
            var cell = Jupyter.notebook.get_cell(cell_idx);

            cell.execute()
            """))

    button.on_click(handle_submit)
    access_key.on_submit(handle_submit)
    secret_key.on_submit(handle_submit)

    raise QuietError(setup_error)
    # raise Exception("No Eigensheep credentials found in AWS credentials profile.")


def show_welcome():
    display(HTML("""
<p>
Prefix any cell with <code>%%eigensheep</code> to run it in AWS Lambda. <a target="_blank" href='""" + GITHUB_URL + """'>Learn more...</a>
</p>
<br />
<details>
<summary>Example: Use `requests` package via Pip</summary>
<pre style="padding-left: 20px">%%eigensheep requests
import requests
requests.get("https://www.google.com").text
</pre>
</details>

<details>
<summary>Example: Run cell 100x concurrently</summary>
<pre style="padding-left: 20px">%%eigensheep -n 100
INDEX + 1 # returns [1, 2, 3, ..., 99, 100]
</pre>
</details>

<details>
<summary>Example: Mapping through an array</summary>
<pre style="padding-left: 20px">%%eigensheep --name do_stuff
DATA + 42
# In a different cell, call `eigensheep.map("do_stuff", [1, 2, 3, 4])`
</pre>
</details>
"""))


@magics_class
class EigensheepMagics(Magics):
    @line_cell_magic
    def es(self, line, cell=None):
        return self.eigensheep(line, cell)

    @line_cell_magic
    def eigensheep(self, line, cell=None):
        args = parser.parse_args(line.split(' '))

        if args.clean_all:
            remove_all_aliases()
            return

        if not cell:
            raise UsageError("Did you accidentally type %eigensheep instead of %%eigensheep?")
        

        deps = [ x for x in args.deps if x ]

        box_config = {
            'requirements': deps,
            'memory': args.memory,
            'timeout': args.timeout,
            'runtime': args.runtime
        }

        alias = make_alias_name(box_config)


        if args.rm or args.reinstall:
            ensure_setup()
            try:
                ali = threadLocal.lambdaClient.get_alias(FunctionName=FUNCTION_NAME, Name=alias)
                threadLocal.lambdaClient.delete_alias(FunctionName=FUNCTION_NAME, Name=ali['Name'])
                threadLocal.lambdaClient.delete_function(FunctionName=FUNCTION_NAME, 
                    Qualifier=ali['FunctionVersion'])
                if alias in known_aliases:
                    known_aliases.remove(alias)
                eprint('Deleted alias "%s".' % alias)

            except threadLocal.lambdaClient.exceptions.ResourceNotFoundException:
                pass
            
            if args.rm:
                return

        if not args.no_install and alias not in known_aliases and not lambda_exists(FUNCTION_NAME, alias):
            ensure_deps(box_config)

        try:
            root = ast.parse(cell)
        except SyntaxError as err:
            raise QuietError(err)

        names = set(node.id for node in ast.walk(root) if isinstance(node, ast.Name))
        exported_vars = names.intersection(ipython.user_ns.keys())
        exported_globals = {}

        for key in exported_vars:
            val = ipython.user_ns[key]
            try:
                json.dumps(val)
                exported_globals[key] = val
            except:
                pass

        run_config = {
            'box': box_config,
            'alias': alias,
            'code': cell,
            'verbose': args.verbose,
            'globals': exported_globals
        }

        if args.name:
            storedLambdas[args.name] = run_config
            eprint('Invoke this stored cell with `eigensheep.invoke("%s")` or `eigensheep.map("%s", [1, 2, ...])`' % (args.name, args.name))
            return None

        if args.n > 1:
            return map(run_config, range(args.n))
        else:
            return invoke(run_config)



def make_alias_name(box_config):
    requirements = sorted([x.lower() for x in set(box_config['requirements'])])
    h = hashlib.sha256(b'1')
    h.update(LAMBDA_TEMPLATE_PYTHON.encode('utf-8'))
    for req in requirements: h.update(req.encode('utf-8'))
    reqs = '_'.join(re.sub('[^\\w]', '', x) for x in requirements)[:50]

    if reqs == '': reqs = 'NO-DEPS'
    return ('%s-%dM-%ds-%s-' % (
        box_config['runtime'].replace('.', ''), 
        box_config['memory'], 
        box_config['timeout'], 
        h.hexdigest()[:5])) + reqs    


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
            sys.stdout.write('WARN: Pickle serialization exceeds 5MB, not returning result.\\n')
            return (False, None)
        return (True, data)
    except Exception:
        return (False, None)

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
        'DATA': pickle.loads(zlib.decompress(base64.b64decode(event['zpickle64'])))
    }
    if 'globals' in event:
        for key in event['globals']:
            globalenv[key] = event['globals'][key]
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


def update_lambda_config(box_config):
    ensure_setup()
    runtime = box_config['runtime']
    memory = box_config['memory']
    timeout = box_config['timeout']
    handler = 'main.lambda_handler'

    threadLocal.lambdaClient.update_function_configuration(
        FunctionName=FUNCTION_NAME,
        Timeout=timeout,
        Runtime=runtime,
        MemorySize=memory,
        Handler=handler,
    )



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


def create_or_update_alias(version, alias):
    ensure_setup()
    try:
        return threadLocal.lambdaClient.update_alias(
            FunctionName=FUNCTION_NAME, Name=alias, FunctionVersion=version)
    except threadLocal.lambdaClient.exceptions.ResourceNotFoundException:    
        return threadLocal.lambdaClient.create_alias(
            FunctionName=FUNCTION_NAME, Name=alias, FunctionVersion=version)
    
def zipstr(ziph, path, contents):    
    info = zipfile.ZipInfo(path)
    info.external_attr = 0o555 << 16 
    ziph.writestr(info, contents)

def build_minimal_lambda_package():
    pseudofile = io.BytesIO()
    zipf = zipfile.ZipFile(pseudofile, 'w', zipfile.ZIP_DEFLATED)
    zipstr(zipf, 'main.py', LAMBDA_TEMPLATE_PYTHON)
    zipf.close()
    return pseudofile.getvalue()

def human_size(bytes, units=[' bytes','KB','MB','GB','TB', 'PB', 'EB']):
    """ Returns a human readable string reprentation of bytes"""
    return str(bytes) + units[0] if bytes < 1024 else human_size(bytes>>10, units[1:])


def ensure_deps(box_config):
    alias = make_alias_name(box_config)
    if lambda_exists(FUNCTION_NAME, alias):
        # eprint("Alias '%s' already exists." % alias)
        return

    if len(box_config['requirements']) == 0:
        package_contents = build_minimal_lambda_package()
        eprint("Uploading package to AWS (%s)..." % human_size(len(package_contents)))
        update_lambda_config(box_config)
        result = threadLocal.lambdaClient.update_function_code(
            FunctionName=FUNCTION_NAME,
            ZipFile=package_contents,
            Publish=True
        )
    else:
        bootstrap_alias = make_alias_name(BOOTSTRAP_CONFIG)
        ensure_deps(BOOTSTRAP_CONFIG)
        eprint("Installing dependencies (this will take a while)...")
        payload = {
            'type': 'BUILD',
            'requirements': box_config['requirements'],
            's3_bucket': bucketID(),
            's3_key': 'lambda_package.zip'
        }
        result = invoke_thread({
            'alias': bootstrap_alias,
            'verbose': False,
            'payload': json.dumps(payload)
        })
        eprint(result['output'])
        update_lambda_config(box_config)
        result = threadLocal.lambdaClient.update_function_code(
            FunctionName=FUNCTION_NAME,
            S3Bucket=payload['s3_bucket'],
            S3Key=payload['s3_key'],
            Publish=True
        )
        # TODO: clean up the S3 package afterwards
        # This isn't a huge problem because we write to the same
        # S3 key each time, so this doesn't actually grow in size. 
    
    create_or_update_alias(result['Version'], alias)
    eprint("Successfully deployed as '%s'." % alias)


def invoke_thread(info):
    ensure_setup()
    result = threadLocal.lambdaClient.invoke(
        FunctionName=FUNCTION_NAME,
        InvocationType='RequestResponse',
        LogType='Tail',
        Payload=info['payload'],
        Qualifier=info['alias']
    )

    known_aliases.add(info['alias'])
    data = json.load(result['Payload'])
    for line in base64.b64decode(result['LogResult']).decode('utf-8').split('\n')[:-1]:
        is_aws = line.startswith('START ') or line.startswith('END ') or line.startswith('REPORT ') or line.startswith('XRAY ')
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
            if 'errorType' in data and data['errorMessage']:
                if data['errorType'] == 'NameError':
                    nameMatch = re.search("(?:name ')([^']+)(?:' is not defined)", data['errorMessage'])
                    if nameMatch:
                        name = nameMatch.group(1)
                        if isinstance(ipython.user_ns.get(name, None), ModuleType):
                            eprint("To use the module '" + name + "' with eigensheep you need to import it in this cell.")
                elif data['errorType'] == 'ModuleNotFoundError':
                    nameMatch = re.search("(?:No module named ')([^']+)(?:')", data['errorMessage'])
                    if nameMatch:
                        name = nameMatch.group(1)
                        eprint("AWS Lambda doesn't include '" + name + "' by default."
                            +"\nTo use '""" + name + "' in Lambda, add the corresponding PyPI package to your eigensheep call above."
                            +"\nThis might look like: '%%eigensheep pypi_package_exporting_""" + name + "'.")
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
            'type': 'RUN',
            'code': run_config['code'],
            'index': i
        }

        if 'globals' in run_config:
            payload['globals'] = run_config['globals']

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



ipython = get_ipython()

setup_error = None
try:
    ensure_setup()
    
except Exception as e:
    setup_error = e

if setup_error:
    show_setup(setup_error)
else:
    show_welcome()

if not hasattr(ipython, 'original_showtraceback'):
    ipython.original_showtraceback = ipython.showtraceback

ipython.showtraceback = hide_traceback
ipython.register_magics(EigensheepMagics)