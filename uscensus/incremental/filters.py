from __future__ import annotations

import re
from typing import TYPE_CHECKING, Callable, Generator, Iterable

if TYPE_CHECKING:
    from uscensus.incremental.wrappers import Dataset


def _make_filter_fn(
        filter_: Callable[[str], bool] | re.Pattern | str
) -> Callable[[str], bool]:
    if isinstance(filter_, str):
        return lambda d: d.find(filter_) >= 0
    if isinstance(filter_, re.Pattern):
        return filter_.match
    return filter_


def filter_datasets(
        datasets: Iterable[Dataset],
        *,
        vintages: list[int] = [],
        title: Callable[[str], bool] | re.Pattern | str = '',
        description: Callable[[str], bool] | re.Pattern | str = '',
        variable: Callable[[str], bool] | re.Pattern | str = '',
        group: Callable[[str], bool] | re.Pattern | str = '',
        tags: Callable[[str], bool] | re.Pattern | str = '',
        geography: Callable[[str], bool] | re.Pattern | str = '',
) -> list[Dataset]:
    if vintages:
        datasets = [dataset for dataset in filter_datasets_vintages(
            datasets, vintages)]
    if title:
        datasets = [dataset for dataset in filter_datasets_title(
            datasets, title)]
    if description:
        datasets = [dataset for dataset in filter_datasets_description(
            datasets, description)]
    if variable:
        datasets = [dataset for dataset in filter_datasets_variables(
            datasets, variable)]
    if group:
        datasets = [dataset for dataset in filter_datasets_groups(
            datasets, group)]
    if tags:
        datasets = [dataset for dataset in filter_datasets_tags(
            datasets, tag)]
    if geography:
        datasets = [dataset for dataset in filter_datasets_geography(
            datasets, geography)]
    return datasets
    

def filter_datasets_vintages(
        datasets: Iterable[Dataset],
        vintages: list[int]
) -> Generator[Dataset, None, None]:
    for dataset in datasets:
        if dataset.c_vintage in vintages:
            yield dataset


def filter_datasets_title(
        datasets: Iterable[Dataset],
        filter_: Callable[[str], bool] | re.Pattern | str        
) -> Generator[Dataset, None, None]:
    filter_fn = _make_filter_fn(filter_)
    for dataset in datasets:
        if filter_fn(dataset.title):
            yield dataset


def filter_datasets_description(
        datasets: Iterable[Dataset],
        filter_: Callable[[str], bool] | re.Pattern | str
) -> Generator[Dataset, None, None]:
    filter_fn = _make_filter_fn(filter_)
    for dataset in datasets:
        if filter_fn(dataset.description):
            yield dataset


def filter_datasets_variables(
        datasets: Iterable[Dataset],
        filter_: Callable[[str], bool] | re.Pattern | str
) -> Generator[Dataset, None, None]:
    filter_fn = _make_filter_fn(filter_)
    for dataset in datasets:
        for variable_name, variable in dataset.variables.items():
            if filter_fn(variable_name) or filter_fn(variable.label):
                yield dataset
                break


def filter_datasets_groups(
        datasets: Iterable[Dataset],
        filter_: Callable[[str], bool] | re.Pattern | str
) -> Generator[Dataset, None, None]:
    filter_fn = _make_filter_fn(filter_)
    for dataset in datasets:
        for group_name, group in dataset.groups.items():
            if (filter_fn(group_name) or filter_fn(group.description) or
                    filter_fn(group.universe)):
                yield dataset
                break


def filter_datasets_tags(
        datasets: Iterable[Dataset],
        filter_: Callable[[str], bool] | re.Pattern | str
) -> Generator[Dataset, None, None]:
    filter_fn = _make_filter_fn(filter_)
    for dataset in datasets:
        for tag in dataset.tags:
            if filter_fn(tag):
                yield dataset
                break


def filter_datasets_geography(
        datasets: Iterable[Dataset],
        level_name: str) -> Generator[Dataset, None, None]:
    for dataset in datasets:
        for level in dataset.geography.levels:
            if level == level_name:
                yield dataset
                break
