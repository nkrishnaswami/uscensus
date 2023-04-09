from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from enum import Enum

import dateutil.parser
from dataclasses_json import DataClassJsonMixin
from dataclasses_json import config as json_config
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


@dataclass(eq=True, frozen=True)
class DefaultLevel(DataClassJsonMixin):
    isDefault: str = ''


@dataclass(eq=True, frozen=True)
class GeographyLevel(DataClassJsonMixin):
    name: str
    geoLevelDisplay: str = ''
    limit: str = ''
    referenceDate: datetime.date | None = _date_field(default=None)
    requires: list[str] = field(default_factory=list)
    wildcard: list[str] = field(default_factory=list)
    optionalWithWCFor: str = ''


@dataclass(eq=True, frozen=True)
class Geography(DataClassJsonMixin):
    fips: list[GeographyLevel] = field(default_factory=list)
    default: list[DefaultLevel] =field(default_factory=list)


@dataclass(eq=True, frozen=True)
class Variable(DataClassJsonMixin):
    label: str
    concept: str = ''
    predicateType: str = ''
    group: str = ''
    limit: int = 0
    predicateOnly: bool | None = None
    hasGeoCollectionSupport: bool | None = None
    attributes: str = ''
    required: str = ''
    universe: str = ''


@dataclass(eq=True, frozen=True)
class Variables(DataClassJsonMixin):
    variables: dict[str, Variable]


@dataclass(eq=True, frozen=True)
class Tags(DataClassJsonMixin):
    tags: list[str]


@dataclass(eq=True, frozen=True)
class Group(DataClassJsonMixin):
    name: str
    description: str
    variables: str
    universe: str = ''


@dataclass(eq=True, frozen=True)
class Groups(DataClassJsonMixin):
    groups: list[Group]


@dataclass(eq=True, frozen=True)
class Sort(DataClassJsonMixin):
    unknown: int


@dataclass(eq=True, frozen=True)
class Sorts(DataClassJsonMixin):
    sorts: list[Sort]


class PublicPrivate(Enum):
    public = 'public'
    private = 'private'


@dataclass(eq=True, frozen=True)
class DcatDistribution(DataClassJsonMixin):
    type_: str = field(metadata=json_config(field_name='@type'))
    accessURL: str
    description: str
    format: str
    mediaType: str
    title: str


@dataclass(eq=True, frozen=True)
class DcatContact(DataClassJsonMixin):
    fn: str
    hasEmail: str


class DcatDatasetType(Enum):
    Dataset = 'dcat:Dataset'


@dataclass(eq=True, frozen=True)
class Dataset(DataClassJsonMixin):
    """A DCAT dataset corresponding to Census data."""

    contactPoint: DcatContact | None = None
    accessLevel: PublicPrivate | None = None
    type_: DcatDatasetType = field(metadata=json_config(field_name='@type'),
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
    c_isCube: bool | None = None
    c_isAggregate: bool | None = None
    c_isAvailable: bool | None = None
    spatial: str | None = None
    temporal: str | None = None


@dataclass(eq=True, frozen=True)
class Catalog(DataClassJsonMixin):
    """A catalog for Census data API calls."""

    context: str = field(metadata=json_config(field_name='@context'))
    id_: str = field(metadata=json_config(field_name='@id'))
    type_: str = field(metadata=json_config(field_name='@type'))
    conformsTo: str
    describedBy: str
    dataset: list[Dataset]
