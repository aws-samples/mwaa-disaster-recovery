# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
