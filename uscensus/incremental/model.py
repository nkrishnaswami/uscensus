from __future__ import annotations

import datetime
from enum import Enum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field


class USCensusBaseModel(BaseModel):
    model_config = ConfigDict(extra='ignore',
                              frozen=True,
                              populate_by_name=True)


class DefaultLevel(USCensusBaseModel):
    isDefault: str = ''


class GeographyLevel(USCensusBaseModel):
    name: str
    geoLevelDisplay: str = ''
    limit: str = ''
    referenceDate: datetime.date | None = None
    requires: list[str] = Field(default_factory=list)
    wildcard: list[str] = Field(default_factory=list)
    optionalWithWCFor: str = ''


class Geography(USCensusBaseModel):
    fips: list[GeographyLevel] = Field(default_factory=list)
    default: list[DefaultLevel] = Field(default_factory=list)


class VariableValueRange(USCensusBaseModel):
    min: int | float
    max: int | float
    description: str


class VariableValues(USCensusBaseModel):
    item: dict[str, str] | None = None
    range: list[VariableValueRange] | None = None


class VariableDatetime(USCensusBaseModel):
    year: bool | None
    month: bool | None
    quarter: bool | None


class Variable(USCensusBaseModel):
    label: str
    concept: str = ''
    predicateType: str = ''
    group: str = ''
    limit: int = 0
    isWeight: Annotated[bool | None, Field(alias='is-weight')] = None
    suggestedWeight: Annotated[str | None,
                               Field(alias='suggested-weight')] = None
    predicateOnly: bool | None = None
    hasGeoCollectionSupport: bool | None = None
    attributes: str = ''
    required: str = ''
    universe: str = ''
    values: VariableValues | None = None
    datetime: VariableDatetime | None = None


class Variables(USCensusBaseModel):
    variables: dict[str, Variable]


class Tags(USCensusBaseModel):
    tags: list[str]


class Group(USCensusBaseModel):
    name: str
    description: str
    variables: str
    universe: str | None = None


class Groups(USCensusBaseModel):
    groups: list[Group]


class Sort(USCensusBaseModel):
    unknown: int


class Sorts(USCensusBaseModel):
    sorts: list[Sort]


class PublicPrivate(Enum):
    public = 'public'
    private = 'private'


class DcatDistribution(USCensusBaseModel):
    type_: Annotated[str, Field('@type')]
    accessURL: str
    description: str
    format: str
    mediaType: str
    title: str


class DcatContact(USCensusBaseModel):
    fn: str
    hasEmail: str


class DcatDatasetType(Enum):
    Dataset = 'dcat:Dataset'


class Dataset(USCensusBaseModel):
    """A DCAT dataset corresponding to Census data."""

    type_: Annotated[DcatDatasetType,
                     Field(alias='@type', default=DcatDatasetType.Dataset)]
    contactPoint: DcatContact | None = None
    accessLevel: PublicPrivate | None = None
    c_dataset: list[str] = Field(default_factory=list)
    c_geographyLink: str = ''
    c_variablesLink: str = ''
    c_examplesLink: str = ''
    c_groupsLink: str = ''
    c_sorts_url: str = ''
    c_documentationLink: str = ''
    title: str = ''
    bureauCode: list[str] = Field(default_factory=list)
    description: str = ''
    distribution: list[DcatDistribution] = Field(default_factory=list)
    identifier: str = ''
    keyword: list[str] = Field(default_factory=list)
    license: str = ''
    programCode: list[str] = Field(default_factory=list)
    references: list[str] = Field(default_factory=list)
    modified: datetime.datetime | None = None
    c_vintage: int | None = None
    c_tagsLink: str | None = None
    c_isMicrodata: bool | None = None
    c_isCube: bool | None = None
    c_isAggregate: bool | None = None
    c_isAvailable: bool | None = None
    spatial: str | None = None
    temporal: str | None = None


class Catalog(USCensusBaseModel):
    """A catalog for Census data API calls."""

    context: Annotated[str, Field(alias='@context')]
    id_: Annotated[str, Field(alias='@id')]
    type_: Annotated[str, Field(alias='@type')]
    conformsTo: str
    describedBy: str
    dataset: list[Dataset]
