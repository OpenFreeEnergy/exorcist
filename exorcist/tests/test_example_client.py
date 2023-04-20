import pytest
import json
import pickle

from exorcist.example_client import (
    ExampleResult, ExampleTaskDetails, ExampleResultStore,
    ExampleTaskDetailsStore,
)

@pytest.fixture
def example_result():
    return ExampleResult(
        label="foo",
        main_result=3.0,
        is_failure=False
    )

@pytest.fixture
def failure_result():
    return ExampleResult(
        label="foo",
        main_result="float division by zero",
        is_failure=True
    )


# simple functions used as our tasks (need to be named functions here
# because pickle doesn't like lambdas)
def incr(x):
    return x + 1.0

def return_1():
    return 1.0

def failure_func():
    return 1.0/0.0


@pytest.fixture
def example_details():
    return ExampleTaskDetails(
        label="foo",
        input_result_labels={},
        task_func=return_1
    )

@pytest.fixture
def result_store(tmp_path):
    return ExampleResultStore(tmp_path)

@pytest.fixture
def task_store(tmp_path):
    return ExampleTaskDetailsStore(tmp_path)


class TestExampleResultStore:
    @pytest.mark.parametrize('fixture', [
        'example_result', 'failure_result'
    ])
    def test_is_failure_result(self, result_store, request, fixture):
        result = request.getfixturevalue(fixture)
        expected = (fixture == 'failure_result')
        assert result_store.is_failure_result(result) == expected

    @pytest.mark.parametrize('fixture', [
        'example_result', 'failure_result'
    ])
    def test_store_result(self, result_store, request, fixture):
        result = request.getfixturevalue(fixture)
        result_store.store_result(result, retry=5)
        filename = {
            "example_result": "foo_result.json",
            "failure_result": "foo_result_5.json",
        }[fixture]
        path = result_store.directory / filename
        assert path.exists()
        with open(path) as f:
            dct = json.load(f)

        recreated = ExampleResult(**dct)
        assert result == recreated


class TestExampleTaskDetailsStore:
    def test_store_load_task_details_cycle(self, task_store,
                                           example_details):
        task_store.store_task_details(example_details.label,
                                      example_details)
        path = task_store.directory / f"{example_details.label}.p"
        assert path.exists()
        reloaded = task_store.load_task_details(example_details.label)
        assert reloaded == example_details

    def test_run_task(self, task_store, example_details):
        result = task_store.run_task(example_details)
        assert not result.is_failure
        assert result.main_result == 1.0
        assert result.label == "foo"

    def test_run_task_failing_result(self, task_store):
        details = ExampleTaskDetails(
            label="baz",
            input_result_labels={},
            task_func=failure_func
        )
        result = task_store.run_task(details)
        assert result.is_failure
        assert result.main_result == "float division by zero"
        assert result.label == "baz"

    def test_workflow(self, task_store, result_store, example_details):
        # depends on store_task_details working
        example_details_2 = ExampleTaskDetails(
            label="bar",
            input_result_labels={'x': "foo"},
            task_func=incr
        )

        # manually do the work of the worker here
        result1 = task_store.run_task(example_details)
        assert not result_store.is_failure_result(result1)
        result_store.store_result(result1)
        result2 = task_store.run_task(example_details_2)
        assert not result_store.is_failure_result(result2)

        assert result2.main_result == 2.0
