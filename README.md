# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/aws-samples/mwaa-disaster-recovery/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                            |    Stmts |     Miss |   Branch |   BrPart |    Cover |   Missing |
|---------------------------------------------------------------- | -------: | -------: | -------: | -------: | -------: | --------: |
| assets/dags/mwaa\_dr/\_\_init\_\_.py                            |        0 |        0 |        0 |        0 |     100% |           |
| assets/dags/mwaa\_dr/framework/\_\_init\_\_.py                  |        0 |        0 |        0 |        0 |     100% |           |
| assets/dags/mwaa\_dr/framework/factory/\_\_init\_\_.py          |        0 |        0 |        0 |        0 |     100% |           |
| assets/dags/mwaa\_dr/framework/factory/base\_dr\_factory.py     |      173 |        0 |       44 |        0 |     100% |           |
| assets/dags/mwaa\_dr/framework/factory/default\_dag\_factory.py |       23 |        0 |        2 |        0 |     100% |           |
| assets/dags/mwaa\_dr/framework/model/\_\_init\_\_.py            |        0 |        0 |        0 |        0 |     100% |           |
| assets/dags/mwaa\_dr/framework/model/active\_dag\_table.py      |       32 |        0 |        8 |        0 |     100% |           |
| assets/dags/mwaa\_dr/framework/model/base\_table.py             |      154 |        0 |       42 |        0 |     100% |           |
| assets/dags/mwaa\_dr/framework/model/connection\_table.py       |       50 |        0 |       22 |        0 |     100% |           |
| assets/dags/mwaa\_dr/framework/model/dependency\_model.py       |       61 |        0 |       28 |        0 |     100% |           |
| assets/dags/mwaa\_dr/framework/model/variable\_table.py         |       44 |        0 |       18 |        0 |     100% |           |
| assets/dags/mwaa\_dr/v\_2\_5/\_\_init\_\_.py                    |        0 |        0 |        0 |        0 |     100% |           |
| assets/dags/mwaa\_dr/v\_2\_5/dr\_factory.py                     |       42 |        0 |        0 |        0 |     100% |           |
| assets/dags/mwaa\_dr/v\_2\_6/\_\_init\_\_.py                    |        0 |        0 |        0 |        0 |     100% |           |
| assets/dags/mwaa\_dr/v\_2\_6/dr\_factory.py                     |        2 |        0 |        0 |        0 |     100% |           |
| assets/dags/mwaa\_dr/v\_2\_7/\_\_init\_\_.py                    |        0 |        0 |        0 |        0 |     100% |           |
| assets/dags/mwaa\_dr/v\_2\_7/dr\_factory.py                     |        8 |        0 |        0 |        0 |     100% |           |
| assets/dags/mwaa\_dr/v\_2\_8/\_\_init\_\_.py                    |        0 |        0 |        0 |        0 |     100% |           |
| assets/dags/mwaa\_dr/v\_2\_8/dr\_factory.py                     |        8 |        0 |        0 |        0 |     100% |           |
| config.py                                                       |      204 |        0 |       94 |        0 |     100% |           |
| lib/\_\_init\_\_.py                                             |        0 |        0 |        0 |        0 |     100% |           |
| lib/dr\_constructs/\_\_init\_\_.py                              |        0 |        0 |        0 |        0 |     100% |           |
| lib/dr\_constructs/airflow\_cli.py                              |       35 |        0 |        5 |        0 |     100% |           |
| lib/functions/\_\_init\_\_.py                                   |        0 |        0 |        0 |        0 |     100% |           |
| lib/functions/airflow\_cli\_client.py                           |      101 |        0 |       33 |        0 |     100% |           |
| lib/functions/airflow\_cli\_function.py                         |       41 |        0 |        8 |        0 |     100% |           |
| lib/functions/airflow\_dag\_trigger\_function.py                |       21 |        0 |        0 |        0 |     100% |           |
| lib/functions/cloudwatch\_health\_check\_function.py            |       33 |        0 |        4 |        0 |     100% |           |
| lib/functions/create\_environment\_function.py                  |       32 |        0 |        6 |        0 |     100% |           |
| lib/functions/disable\_scheduler\_function.py                   |       19 |        0 |        2 |        0 |     100% |           |
| lib/functions/get\_environment\_function.py                     |       14 |        0 |        0 |        0 |     100% |           |
| lib/functions/replication\_job\_function.py                     |       19 |        0 |        2 |        0 |     100% |           |
| lib/stacks/\_\_init\_\_.py                                      |        0 |        0 |        0 |        0 |     100% |           |
| lib/stacks/mwaa\_base\_stack.py                                 |       39 |        0 |       10 |        0 |     100% |           |
| lib/stacks/mwaa\_primary\_stack.py                              |      144 |        0 |       12 |        0 |     100% |           |
| lib/stacks/mwaa\_secondary\_stack.py                            |      163 |        0 |       14 |        0 |     100% |           |
|                                                       **TOTAL** | **1462** |    **0** |  **354** |    **0** | **100%** |           |


## Setup coverage badge

Below are examples of the badges you can use in your main branch `README` file.

### Direct image

[![Coverage badge](https://raw.githubusercontent.com/aws-samples/mwaa-disaster-recovery/python-coverage-comment-action-data/badge.svg)](https://htmlpreview.github.io/?https://github.com/aws-samples/mwaa-disaster-recovery/blob/python-coverage-comment-action-data/htmlcov/index.html)

This is the one to use if your repository is private or if you don't want to customize anything.

### [Shields.io](https://shields.io) Json Endpoint

[![Coverage badge](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/aws-samples/mwaa-disaster-recovery/python-coverage-comment-action-data/endpoint.json)](https://htmlpreview.github.io/?https://github.com/aws-samples/mwaa-disaster-recovery/blob/python-coverage-comment-action-data/htmlcov/index.html)

Using this one will allow you to [customize](https://shields.io/endpoint) the look of your badge.
It won't work with private repositories. It won't be refreshed more than once per five minutes.

### [Shields.io](https://shields.io) Dynamic Badge

[![Coverage badge](https://img.shields.io/badge/dynamic/json?color=brightgreen&label=coverage&query=%24.message&url=https%3A%2F%2Fraw.githubusercontent.com%2Faws-samples%2Fmwaa-disaster-recovery%2Fpython-coverage-comment-action-data%2Fendpoint.json)](https://htmlpreview.github.io/?https://github.com/aws-samples/mwaa-disaster-recovery/blob/python-coverage-comment-action-data/htmlcov/index.html)

This one will always be the same color. It won't work for private repos. I'm not even sure why we included it.

## What is that?

This branch is part of the
[python-coverage-comment-action](https://github.com/marketplace/actions/python-coverage-comment)
GitHub Action. All the files in this branch are automatically generated and may be
overwritten at any moment.