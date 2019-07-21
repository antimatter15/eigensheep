<img src="https://raw.githubusercontent.com/antimatter15/lambdu/master/images/logo.png" alt="eigensheep" width="500"/>

![PyPI](https://img.shields.io/pypi/v/eigensheep.svg)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/eigensheep.svg)
![PyPI - License](https://img.shields.io/pypi/l/eigensheep.svg)

Eigensheep lets you easily run cells in Jupyter Notebooks on AWS Lambda with massive parallelism. You can instantly provision and run your code on 1000 different tiny VMs by simply prefixing a cell with `%%eigensheep -n 1000`. 

## Getting Started

Open up your Terminal and install `eigensheep` with `pip`

    pip3 install eigensheep

Open a Jupyter notebook with `jupyter notebook` and create a new Python 3 notebook. Run the following code in a cell:

    import eigensheep

Follow the on-screen instructions to configure AWS credentials. Eigensheep uses AWS CloudFormation so you only need to a few clicks to get started. 

<img src="https://raw.githubusercontent.com/antimatter15/lambdu/master/images/setup.png" alt="eigensheep setup" width="500" />

Once Eigensheep is set up, you can run any code on Lambda by prefixing the cell with `%%eigensheep`. You can include dependencies from `pip` by typing `%%eigensheep requests numpy`. You can invoke a cell multiple times concurrently with `%%eigensheep -n 100`. 

<img src="https://raw.githubusercontent.com/antimatter15/lambdu/master/images/parallel.gif" alt="eigensheep usage" width="500"  />

## Frequently Asked Questions

*Q: Why is this library called Eigensheep?*

The name comes from the classic math joke:

> What do you call a baby eigensheep? 
> 
> A [lamb, duh](https://en.wikipedia.org/wiki/Eigenvalues_and_eigenvectors#Overview). 

*Q: Does this work on Python 2 and Python 3?*

A: Both Python 2 and Python 3 are supported. If the library is imported from a Python 2.x notebook, the Lambda runtime will default to "python2.6". If the library is imported from a Python 3.x notebook, the Lambda runtime defaults to "python3.6". This can be manually overridden with the "--runtime" option.

*Q: Can I use this to do GPU stuff?*

A: Currently the AWS Lambda execution environment does not expose access to any GPU acceleration. Eigensheep probably won't be that useful for training deep neural nets.

*Q: How much does it cost to run stuff on AWS Lambda?*

A: Unlike a traditional VM, you don't get charged while you're idling and not actively computing. You don't have to worry about accidentally forgetting to turn off a machine, and provisioning a VM takes only milliseconds rather than minutes. 

AWS provides a pretty generous Free Tier for Lambda which does not expire after 12 months. It's 400,000 GB-seconds/month. That's 36 continuous hours of a single maxed out 3108MB Lambda job for free every month. Alternatively, it's about 20 minutes of 100 concurrent maxed out instances. After that it's about $7 for every subsequent free-tier equivalent. 

*Q: Can this be used for web scraping?*

A: Yes, Eigensheep can be used for web scraping. However, note that different Lambda VM instances often share the same IP address. 

*Q: Can Eigensheep be used for long running computations?*

A: The maximum allowed duration of any Lambda job is 15 minutes. Eigensheep works best for tasks which can be broken up into smaller chunks. 


*Q: What are the security implications of using Eigensheep?*

A: The Eigensheep CloudFormation stack creates an IAM User, Access Key, and Lambda Role with as few permissions as possible. If the access keys are compromised, the attacker only has access to a bucket containing Eigensheep-specific content, and can not use it to access any of your other AWS resources. 

The IAM User can only read/write from a specific bucket earmarked for use with Eigensheep, and can only update a specific lambda function (all the different variants are stored as different versions on a single Lambda function). The Lambda function only has access to the specific bucket and the ability to write to CloudWatch logs and XRay tracing streams. 

All of the access keys can be revoked and all of the resources can be removed simply by deleting the CloudFormation stack from the AWS console. 


*Q: Where does Eigensheep store its configuration?*

A: Eigensheep stores its access keys and configuration in the `~/.aws/config` file under the `eigensheep` profile.


*Q: Can I use Eigensheep without installing the CloudFormation Stack?*

A: Yes. Although it's a bit more complicated to set up. You can use any AWS access key and secret, so long as it has the ability to modify/invoke a Lambda named "EigensheepLambda" (which must be manually created). You must also create an S3 bucket named "eigensheep-YOUR_ACCOUNT_ID", where YOUR_ACCOUNT_ID is your numerical AWS account ID.

## Usage

```
usage: %%eigensheep [-h] [--memory MEMORY] [--timeout TIMEOUT] [--no_install]
                    [--clean] [--rm] [--reinstall] [--runtime RUNTIME]
                    [-n N] [--verbose] [--name NAME]
                    [deps [deps ...]]

Jupyter cell magic to invoke cell on AWS Lambda

positional arguments:
  deps               dependencies to be installed via PyPI

optional arguments:
  -h, --help         show this help message and exit
  --memory MEMORY    amount of memory in 64MB increments from 128 up to 3008
  --timeout TIMEOUT  lambda execution timeout in seconds up to 900 (15
                     minutes)
  --no_install       do not install dependencies if not found
  --clean        remove all deployed dependencies
  --rm               remove a specific
  --reinstall        uninstall and reinstall
  --runtime RUNTIME  which runtime (python3.6, python2.7)
  -n N               number of lambdas to invoke
  --verbose          show additional information from lambda invocation
  --name NAME        name to store this lambda as
```


`eigensheep.map("do_stuff", [1, 2, 3, 4])`


`eigensheep.invoke("do_stuff")`



```
%eigensheep --clean
```


## Acknowledgements

This library was written by [Kevin Kwok](https://twitter.com/antimatter15) and [Guillermo Webster](https://twitter.com/biject). It is based on Jupyter/IPython, `tqdm`, `boto3`, and countless Stackoverflow answers.

If you're interested in this project, you should also check out [PyWren](http://pywren.io/) by Eric Jonas, and [ExCamera](https://www.usenix.org/system/files/conference/nsdi17/nsdi17-fouladi.pdf) from Sadjad Fouladi, et al. 
