"""Helpers to filter a list of datasets in some convenient ways."""
from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable

    from uscensus.incremental.wrappers import Dataset

    FilterSpec = Callable[[str], bool] | re.Pattern | str
    StringPredicate = Callable[[str], bool]
    DatasetGenerator = Generator[Dataset, None, None]


def _make_filter_fn(
        filter_spec: FilterSpec,
) -> StringPredicate:
    if isinstance(filter_spec, str):
        return lambda d: d.find(filter_spec) >= 0
    if isinstance(filter_spec, re.Pattern):
        return lambda d: filter_spec.search(d) is not None
    return filter_spec


def _make_filter_eq_fn(
        filter_spec: FilterSpec,
) -> StringPredicate:
    if isinstance(filter_spec, str):
        return lambda d: d == filter_spec
    if isinstance(filter_spec, re.Pattern):
        return lambda d: filter_spec.fullmatch(d) is not None
    return filter_spec


def filter_datasets(
        datasets: Iterable[Dataset],
        *,
        vintages: list[int] = [],
        title: FilterSpec = '',
        description: FilterSpec = '',
        variable: FilterSpec = '',
        group: FilterSpec = '',
        tags: FilterSpec = '',
        geography: FilterSpec = '',
) -> list[Dataset]:
    if vintages:
        datasets = list(filter_datasets_vintages(
            datasets, vintages))
    if title:
        datasets = list(filter_datasets_title(
            datasets, title))
    if description:
        datasets = list(filter_datasets_description(
            datasets, description))
    if variable:
        datasets = list(filter_datasets_variables(
            datasets, variable))
    if group:
        datasets = list(filter_datasets_groups(
            datasets, group))
    if tags:
        datasets = list(filter_datasets_tags(
            datasets, tags))
    if geography:
        datasets = list(filter_datasets_geography(
            datasets, geography))
    return list(datasets)


def filter_datasets_vintages(
        datasets: Iterable[Dataset],
        vintages: list[int],
) -> DatasetGenerator:
    for dataset in datasets:
        if dataset.c_vintage in vintages:
            yield dataset


def filter_datasets_title(
        datasets: Iterable[Dataset],
        filter_: FilterSpec,
) -> DatasetGenerator:
    filter_fn = _make_filter_fn(filter_)
    for dataset in datasets:
        if filter_fn(dataset.title):
            yield dataset


def filter_datasets_description(
        datasets: Iterable[Dataset],
        filter_: FilterSpec,
) -> DatasetGenerator:
    filter_fn = _make_filter_fn(filter_)
    for dataset in datasets:
        if filter_fn(dataset.description):
            yield dataset


def filter_datasets_variables(
        datasets: Iterable[Dataset],
        filter_: FilterSpec,
) -> DatasetGenerator:
    filter_fn = _make_filter_fn(filter_)
    for dataset in datasets:
        for variable_name, variable in dataset.variables.items():
            if filter_fn(variable_name) or filter_fn(variable.label):
                yield dataset
                break


def filter_datasets_groups(
        datasets: Iterable[Dataset],
        filter_: FilterSpec,
) -> DatasetGenerator:
    filter_fn = _make_filter_fn(filter_)
    for dataset in datasets:
        for group_name, group in dataset.groups.items():
            if (filter_fn(group_name) or filter_fn(group.description) or
                    filter_fn(group.universe)):
                yield dataset
                break


def filter_datasets_tags(
        datasets: Iterable[Dataset],
        filter_: FilterSpec,
) -> DatasetGenerator:
    filter_fn = _make_filter_fn(filter_)
    for dataset in datasets:
        for tag in dataset.tags:
            if filter_fn(tag):
                yield dataset
                break


def filter_datasets_geography(
        datasets: Iterable[Dataset],
        filter_: FilterSpec) -> DatasetGenerator:
    filter_fn = _make_filter_eq_fn(filter_)
    for dataset in datasets:
        for level in dataset.geography.levels:
            if filter_fn(level):
                yield dataset
                break
