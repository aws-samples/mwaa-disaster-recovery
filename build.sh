#!/usr/bin/env bash

: '
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
'

versions=("2_5" "2_6" "2_7" "2_8")

v_2_5="2.5.1"
v_2_6="2.6.3"
v_2_7="2.7.2"
v_2_8="2.8.1"

previous_branch=""
current_branch=""


prompt() {
    echo "MWAA Disaster Recovery Build Script"
    echo "====================================="
    echo "Syntax: build.sh <commmand> <arg1> ... <argn>"
    echo "Commands:"
    echo "  clean: Removes build artifacts"
    echo "      e.g. $./build.sh clean"
    echo "  lint: Runs linter"
    echo "      e.g. $./build.sh lint"
    echo "  unit: Runs all unit tests by default. Takes a path as an optional argument to run test cases in the supplied path."
    echo "      e.g. $./build.sh unit"
    echo "      e.g. $./build.sh unit tests/unit/mwaa_dr"
    echo "  integration: Runs all integration tests. (Under Development)"
    echo "      e.g. $./build.sh integration"
    echo "  setup: Setups up integration test environment by starting mwaa local runner container. Takes mwaa version as an argument -- one of [2_5, 2_6, 2_7, 2_8]."
    echo "      e.g. $./build.sh setup 2_8"
    echo "  teardown: Teardowns integration test environment by stopping mwaa local runner container. Takes mwaa version as an argument -- one of [2_5, 2_6, 2_7, 2_8]."
    echo "      e.g. $./build.sh teardown 2_8"
    echo "  help: Presents the build options"
    echo "      e.g. $./build.sh help"
    echo "====================================="
}


clean() {
    rm -rf .venv
    rm -rf .pytest_cache
    rm -rf build
    rm -rf cdk.out
    find . | grep -E "(/__pycache__$|\.pyc$|\.pyo$|\.DS_Store$)" | xargs rm -rf
}


lint() {
    pre-commit run --all-files
}


unit_test() {
    path="${1:-tests/unit}"
    rm -rf coverage
    mkdir -p coverage
    coverage run -m pytest $path && coverage report -m
    find . | grep -E "(/__pycache__$|\.pyc$|\.pyo$|\.DS_Store$)" | xargs rm -rf
}


check_image() {
    version=$1

    # Check if version is valid
    if [[ ! " ${versions[@]} " =~ " ${version} " ]]; then
        echo "Unsupported MWAA version: $version! Currently supported version are: ${versions[@]}!"
        return 2
    fi

    # Check if image exists locally
    semver="v_$version"
    echo "Checking docker image for MWAA version ${!semver} ..."
    image="amazon/mwaa-local:$version"
    if docker image inspect $image > /dev/null 2>&1; then
        echo "Image $image found locally!"
        return 0
    fi
    echo "Image $image not found locally!"
    return 1
}


build_image() {
    version=$1
    semver="v_$version"

    echo "Building docker image for MWAA version ${!semver} ..."

    cd aws-mwaa-local-runner
    git fetch
    previous_branch=$(git rev-parse --abbrev-ref HEAD)
    current_branch="v${!semver}"

    echo "Switching from branch $previous_branch to $current_branch ..."
    git checkout $current_branch

    cp -f ../assets/requirements.txt ./requirements
    ./mwaa-local-env build-image

    echo "Switching from branch $current_branch to $previous_branch ..."
    git reset --hard
    git checkout $previous_branch
    cd ..

    echo "Completed building docker image for MWAA version ${!semver} ..."
}


start_mwaa() {
    version=$1
    semver="v_$version"

    echo "Starting MWAA version ${!semver} locally ..."

    cd aws-mwaa-local-runner
    echo "Resetting the MWAA local runner project for integration testing ..."
    git reset --hard
    git fetch
    previous_branch=$(git rev-parse --abbrev-ref HEAD)
    current_branch="v${!semver}"

    echo "Switching from branch $previous_branch to $current_branch ..."
    git checkout $current_branch

    rm -rf db-data
    rm -rf dags/*
    cp -f ../assets/requirements.txt ./requirements
    cp -r ../assets/dags/* ./dags
    cp -r ../tests/integration/dags/* ./dags
    cp -r ../tests/integration/startup_script/startup.sh ./startup_script
    mkdir -p dags/data
    cp -r ../tests/integration/data/v_$version/* ./dags/data

    DOCKER_COMPOSE_PROJECT_NAME=aws-mwaa-local-runner-$version
    docker-compose -p $DOCKER_COMPOSE_PROJECT_NAME -f ./docker/docker-compose-local.yml up -d --wait
}


stop_mwaa() {
    version=$1
    semver="v_$version"

    echo "Stopping local MWAA version ${!semver} ..."
    cd aws-mwaa-local-runner

    DOCKER_COMPOSE_PROJECT_NAME=aws-mwaa-local-runner-$version
    docker-compose -p $DOCKER_COMPOSE_PROJECT_NAME -f ./docker/docker-compose-local.yml down
    cd ..
    echo "MWAA version ${!semver} stopped!"
}


integration_test_setup() {
    version=$1
    echo "Setting up integration tests for MWAA version $version ..."

    check_image $version
    local result=$?

    if [ "$result" -eq 2 ]; then
        return 2
    elif [ "$result" -eq 1 ]; then
        build_image $version
    fi

    start_mwaa $version
}


integration_test_teardown() {
    version=$1
    stop_mwaa $version
}

integration_test() {
    echo "Integration test feature is under development!"
    # pytest -rP tests/integration
}

trigger_dag() {
    dag_name=$1
}

# Driver
case "$1" in
    clean)
        clean
    ;;
    lint)
        lint
    ;;
    unit)
        unit_test $2
    ;;
    integration)
        integration_test
    ;;
    setup)
        integration_test_setup $2
    ;;
    teardown)
        integration_test_teardown $2
    ;;
    *)
        prompt
esac
