# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- Unit tests for functions, CDK stacks, and CDK constructs

### Changed
- Updated the DAG factory for 2.8 to add new fields in the log and task instance tables
- Bugfix for the Airflow CLI client
- Updated the linting script to exclude `isort` as its changes conflicted with `black`
- Updated the README to include FAQ section
- Renamed `lib/function` folder to `lib/functions` to be consistent with other folder



## [0.1.1] - 2024-04-22
### Added

- The [mwaa_dr/](assets/dags/mwaa_dr/) framework for supporting multiple versions of MWAA
 with minimal code changes
- CDK [constructs](lib/constructs/) to issue Airflow cli commands during stack deployments
- CDK [stacks](lib/stacks/) to deploy resources in both primary and secondary AWS regions
 for supporting DR
- AWS Lambda [functions](lib/function/) for supporting cli commands and DR workflows
- [config.py](./config.py) to support stack parameters and other project configurations

### Changed

- README to include project information
- Contribution guide with info on how to contribute


## [0.0.0] - 2024-04-19

### Added

- Initial commit with sample readme, code of conduct, and license


[unreleased]: https://github.com/aws-samples/mwaa-disaster-recovery/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/aws-samples/mwaa-disaster-recovery/compare/v0.0.0...v0.1.1
[0.0.0]: https://github.com/aws-samples/mwaa-disaster-recovery/releases/tag/v0.0.0
