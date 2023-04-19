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
    def test_store_task_details(self, task_store, example_details):
        task_store.store_task_details(example_details.label,
                                      example_details)
        path = task_store.directory / f"{example_details.label}.p"
        assert path.exists()
        with open(path, mode='rb') as f:
            reloaded = pickle.load(f)

        assert reloaded == example_details


    def test_load_task(self, task_store, example_details):
        # note that this is dependent on store_task_details working, not
        # quite true unit
        task_store.store_task_details(example_details.label,
                                      example_details)
        task = task_store.load_task(example_details.label)
        result = task()
        assert not result.is_failure
        assert result.main_result == 1.0
        assert result.label == "foo"

    def test_workflow(self, task_store, result_store, example_details):
        # depends on store_task_details working
        example_details_2 = ExampleTaskDetails(
            label="bar",
            input_result_labels={'x': "foo"},
            task_func=incr
        )

        # the order of storage shouldn't matter, so we intentionally store
        # in the inverted order in this test
        task_store.store_task_details(example_details_2.label,
                                      example_details_2)
        task_store.store_task_details(example_details.label,
                                      example_details)

        # order in which we load them also doesn't matter -- only the order
        # in which we run them does
        task2 = task_store.load_task("bar")
        task = task_store.load_task("foo")

        # manually do the work of the worker here
        result1 = task()
        assert not result_store.is_failure_result(result1)
        result_store.store_result(result1)
        result2 = task2()
        assert not result_store.is_failure_result(result2)

        assert result2.main_result == 2.0
