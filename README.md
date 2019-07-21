<img src="https://raw.githubusercontent.com/antimatter15/lambdu/master/logo.png" alt="eigensheep"/>

## Getting Started

Open up your Terminal and install `eigensheep` with `pip`

    pip3 install eigensheep

Open a Jupyter notebook with `jupyter notebook` and create a new Python 3 notebook. Run the following code in a cell:

    import eigensheep

Follow the on-screen instructions to configure AWS credentials. Eigensheep uses AWS CloudFormation so you only need to a few clicks to get started. 

Once Eigensheep is set up, you can run any code on Lambda by prefixing the cell with `%%eigensheep`. You can include dependencies from `pip` by typing `%%eigensheep requests numpy`. You can invoke a cell multiple times concurrently with `%%eigensheep -n 100`. 


