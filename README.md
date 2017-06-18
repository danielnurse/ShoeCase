# ShoeCasus
Shoe Casus

## Setup

- Install Python 2.7
- Create virtualenv [Optional, but recommended]
- install project requirements:
    ```pip install -r requirements.txt```


## Import datasets

- place dataset files in "./dataset/" folder
- assign filepaths in "config.py"
- run import command:
    ```python stride/importer.py```
    or
    ```cd stride; python -m 'importer'```


## Start Restfull API Server

- run server command
    ```python stride/app.py```
    or
    ```cd stride; python -m 'app'```
