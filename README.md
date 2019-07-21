<img src="https://raw.githubusercontent.com/antimatter15/lambdu/master/images/logo.png" alt="eigensheep"/>

Eigensheep lets you easily run cells in Jupyter Notebooks on AWS Lambda with massive parallelism. You can instantly provision and run your code on 1000 different tiny VMs by simply prefixing a cell with `%%eigensheep -n 1000`. 

## Getting Started

Open up your Terminal and install `eigensheep` with `pip`

    pip3 install eigensheep

Open a Jupyter notebook with `jupyter notebook` and create a new Python 3 notebook. Run the following code in a cell:

    import eigensheep

Follow the on-screen instructions to configure AWS credentials. Eigensheep uses AWS CloudFormation so you only need to a few clicks to get started. 

<img src="https://raw.githubusercontent.com/antimatter15/lambdu/master/images/setup.png" alt="eigensheep setup"/>

Once Eigensheep is set up, you can run any code on Lambda by prefixing the cell with `%%eigensheep`. You can include dependencies from `pip` by typing `%%eigensheep requests numpy`. You can invoke a cell multiple times concurrently with `%%eigensheep -n 100`. 

<img src="https://raw.githubusercontent.com/antimatter15/lambdu/master/images/parallel.gif" alt="eigensheep setup"/>

## Acknowledgements

This library was written by Kevin Kwok and Guillermo Webster. It is based on Jupyter/IPython, `tqdm`, `boto3`, and countless Stackoverflow answers.

Eigensheep was inspired by [ExCamera](https://www.usenix.org/system/files/conference/nsdi17/nsdi17-fouladi.pdf) by Sadjad Fouladi, et al. 

If you're interested in this project, you should also check out [PyWren](http://pywren.io/) by Eric Jonas. 
