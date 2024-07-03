# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2024-07-03
### Added
- Cross region replication of existing DAGs S3 bucket objects from primary to secondary during primary stack deployment
- Clean up of the metadata store before restore operation for the `WARM_STANDBY` deployment
- Customizable recovery strategy for `variable` and `connection`

### Changed
- Updates to the `cleanup_metadata` dag to include additional tables: `task_fail`, `trigger`, and `slot_pool`
- Documentation and test cases updates to reflect the changes

## [0.4.0] - 2024-06-26
### Added
- Added support for cleaning up the metadata store before running restore operations as a part of the automated restore StepFunctions workflow
- Introduced a new stack parameters, `METADATA_CLEANUP_DAG_NAME`, to represent the name of metadata clean up dag for restore operations
- Introduced a new stack parameters, `SECONDARY_CLEANUP_COOL_OFF_SECS`, to represent the cool of time in secs between the metadata store cleanup operation and the restore operation in the recovery workflow

## Changed
- Updates the `README` and `PYPIDOC` reflecting addition of metadata cleanup DAG
- Updates to `pyproject.toml` to properly report uncovered files during unit testing

## [0.3.0] - 2024-06-25
### Changed
- Bugfixes to the [cleanup_metadata](assets/dags/mwaa_dr/cleanup_metadata.py) DAG
- Bugfixes to the [functions](lib/functions/) module
- Performance optimization of the restore operation -- Moved away from the two-step process of first downloading the file locally and then loading the file into metadata store to directly streaming the file from S3. Introduces API spec changes to `BaseTable`
- Updates to `build.sh` script to include publishing commands
- Updates to `README` and `PYPIDOC`

## [0.2.4] - 2024-06-19
### Changed
- Updating `backup_matadata` and `restore_metadata` script to use the new framework access pattern
- Updated `PYPIDOC` to include the backup and restore workflow sample runs


## [0.2.3] - 2024-06-19
### Changed
- Further fix for the DAG detection bug while using the `mwaa-dr` library in client DAGs
- Updated the examples and documentation in `PYPIDOC` to reflect the new usage pattern


## [0.2.2] - 2024-06-19
### Changed
- Fixed the DAG detection bug while using the `mwaa-dr` library in client DAGs
- Updated the examples and documentation in `README` and `PYPIDOC` to reflect the new usage pattern


## [0.2.1] - 2024-06-18
### Changed
- Fixed the bug in release pipeline to only deploy to TestPyPi package repo on non-tag push
- Updated the `README` and `PYPIDOC` to include pypi package version badge.


## [0.2.0] - 2024-06-18
### Added
- Unit tests for functions, CDK stacks, and CDK constructs
- Build script added for linting and running tests
- CICD pipeline and `mwaa_dr` package publishing in [PyPI](https://pypi.org/project/mwaa-dr/)

### Changed
- Updated the DAG factory for 2.8 to add new fields in the log and task instance tables
- Bugfix for the Airflow CLI client
- Updated the linting script to exclude `isort` as its changes conflicted with `black`
- Updated the `README` to include FAQ section
- Renamed `lib/function` folder to `lib/functions` to be consistent with other folder
- Update to `pyproject.toml` file to set related_files to `true` for coverage reporting

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


[unreleased]: https://github.com/aws-samples/mwaa-disaster-recovery/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/aws-samples/mwaa-disaster-recovery/compare/v0.4.0...v1.0.0
[0.4.0]: https://github.com/aws-samples/mwaa-disaster-recovery/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/aws-samples/mwaa-disaster-recovery/compare/v0.2.4...v0.3.0
[0.2.4]: https://github.com/aws-samples/mwaa-disaster-recovery/compare/v0.2.3...v0.2.4
[0.2.3]: https://github.com/aws-samples/mwaa-disaster-recovery/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/aws-samples/mwaa-disaster-recovery/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/aws-samples/mwaa-disaster-recovery/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/aws-samples/mwaa-disaster-recovery/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/aws-samples/mwaa-disaster-recovery/compare/v0.0.0...v0.1.1
[0.0.0]: https://github.com/aws-samples/mwaa-disaster-recovery/releases/tag/v0.0.0
