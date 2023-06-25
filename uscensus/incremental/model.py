from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from enum import Enum

import dateutil.parser
from dataclasses_json import dataclass_json, config as json_config, Undefined
from marshmallow import fields as mm_fields


def _date_field(**kwargs):
    return field(
        metadata=json_config(
            encoder=lambda x: datetime.date.isoformat(x) if x else None,
            decoder=lambda x: datetime.date.fromisoformat(x) if x else None,
            mm_field=mm_fields.DateTime(format='iso'),
        ),
        **kwargs)


def _datetime_field(**kwargs):
    return field(
        metadata=json_config(
            encoder=datetime.datetime.isoformat,
            decoder=dateutil.parser.parse,
            mm_field=mm_fields.DateTime(format='iso'),
        ),
        **kwargs)


def _renamed_field(field_name, **kwargs):
    return field(
        metadata=json_config(field_name=field_name), **kwargs)


@dataclass_json(undefined=Undefined.RAISE)
@dataclass(eq=True, frozen=True)
class DefaultLevel:
    isDefault: str = ''


@dataclass_json(undefined=Undefined.RAISE)
@dataclass(eq=True, frozen=True)
class GeographyLevel:
    name: str
    geoLevelDisplay: str = ''
    limit: str = ''
    referenceDate: datetime.date | None = _date_field(default=None)
    requires: list[str] = field(default_factory=list)
    wildcard: list[str] = field(default_factory=list)
    optionalWithWCFor: str = ''


@dataclass_json(undefined=Undefined.RAISE)
@dataclass(eq=True, frozen=True)
class Geography:
    fips: list[GeographyLevel] = field(default_factory=list)
    default: list[DefaultLevel] = field(default_factory=list)


@dataclass_json(undefined=Undefined.RAISE)
@dataclass(eq=True, frozen=True)
class VariableValueRange:
    min: int | float
    max: int | float
    description: str
    

@dataclass_json(undefined=Undefined.RAISE)
@dataclass(eq=True, frozen=True)
class VariableValues:
    item: dict[str, str] | None = None
    range: list[VariableValueRange] | None = None


@dataclass_json(undefined=Undefined.RAISE)
@dataclass(eq=True, frozen=True)
class VariableDatetime:
    year: bool | None
    month: bool | None
    quarter: bool | None
    

@dataclass_json(undefined=Undefined.RAISE)
@dataclass(eq=True, frozen=True)
class Variable:
    label: str
    concept: str = ''
    predicateType: str = ''
    group: str = ''
    limit: int = 0
    isWeight: bool | None = _renamed_field('is-weight', default=None)
    suggestedWeight: str | None = _renamed_field('suggested-weight', default=None)
    predicateOnly: bool | None = None
    hasGeoCollectionSupport: bool | None = None
    attributes: str = ''
    required: str = ''
    universe: str = ''
    values: VariableValues | None = None
    datetime: VariableDatetime | None = None


@dataclass_json(undefined=Undefined.RAISE)
@dataclass(eq=True, frozen=True)
class Variables:
    variables: dict[str, Variable]


@dataclass_json(undefined=Undefined.RAISE)
@dataclass(eq=True, frozen=True)
class Tags:
    tags: list[str]


@dataclass_json(undefined=Undefined.RAISE)
@dataclass(eq=True, frozen=True)
class Group:
    name: str
    description: str
    variables: str
    universe: str


@dataclass_json(undefined=Undefined.RAISE)
@dataclass(eq=True, frozen=True)
class Groups:
    groups: list[Group]


@dataclass_json(undefined=Undefined.RAISE)
@dataclass(eq=True, frozen=True)
class Sort:
    unknown: int


@dataclass_json(undefined=Undefined.RAISE)
@dataclass(eq=True, frozen=True)
class Sorts:
    sorts: list[Sort]


class PublicPrivate(Enum):
    public = 'public'
    private = 'private'


@dataclass_json(undefined=Undefined.RAISE)
@dataclass(eq=True, frozen=True)
class DcatDistribution:
    type_: str = _renamed_field('@type')
    accessURL: str
    description: str
    format: str
    mediaType: str
    title: str


@dataclass_json(undefined=Undefined.RAISE)
@dataclass(eq=True, frozen=True)
class DcatContact:
    fn: str
    hasEmail: str


class DcatDatasetType(Enum):
    Dataset = 'dcat:Dataset'


@dataclass_json
@dataclass(eq=True, frozen=True)
class Dataset:
    """A DCAT dataset corresponding to Census data."""

    contactPoint: DcatContact | None = None
    accessLevel: PublicPrivate | None = None
    type_: DcatDatasetType = _renamed_field('@type',
                                            default=DcatDatasetType.Dataset)
    c_dataset: list[str] = field(default_factory=list)
    c_geographyLink: str = ''
    c_variablesLink: str = ''
    c_examplesLink: str = ''
    c_groupsLink: str = ''
    c_sorts_url: str = ''
    c_documentationLink: str = ''
    title: str = ''
    bureauCode: list[str] = field(default_factory=list)
    description: str = ''
    distribution: list[DcatDistribution] = field(default_factory=list)
    identifier: str = ''
    keyword: list[str] = field(default_factory=list)
    license: str = ''
    programCode: list[str] = field(default_factory=list)
    references: list[str] = field(default_factory=list)
    modified: datetime.datetime | None = _datetime_field(default=None)
    c_vintage: int | None = None
    c_tagsLink: str | None = None
    c_isMicrodata: bool | None = None
    c_isCube: bool | None = None
    c_isAggregate: bool | None = None
    c_isAvailable: bool | None = None
    spatial: str | None = None
    temporal: str | None = None


@dataclass_json
@dataclass(eq=True, frozen=True)
class Catalog:
    """A catalog for Census data API calls."""

    context: str = _renamed_field('@context')
    id_: str = _renamed_field('@id')
    type_: str = _renamed_field('@type')
    conformsTo: str
    describedBy: str
    dataset: list[Dataset]
