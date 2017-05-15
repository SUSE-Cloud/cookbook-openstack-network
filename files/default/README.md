# Unit Testing

Make a python 2 virtual environment, activate it, and install the requirements:

    pip install -r test-requirements.txt

Run the tests

    coverage run $(which nosetests) .

Coverage report

    coverage report -i $(pwd)/*.py

Code analysis

    flake8 *.py
