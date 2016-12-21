# Unit Testing

Make a python 2 virtual environment, activate it, and install the requirements:

    pip install -r test-requirements.txt

Run the tests

    coverage run test-neutron-ha-tool.py

Coverage report

    coverage report -i neutron-ha-tool.py

Code analysis

	flake8 neutron-ha-tool.py test-neutron-ha-tool.py
