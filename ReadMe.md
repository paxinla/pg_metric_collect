# postgresql metric to riemann 

An example for collecting metric of postgresql and os , then sending them to riemann.

## Usage

Run `make init` to install a virtual python environment, this require python, pip and virtualenv install on your machine.

Run `source ENV/bin/activate` to activate the virtual environment, then use `python -m pg_metric_collect.main --conf SomePath/config.ini` to start the collector. An example configuration file provide as config_example.ini .
