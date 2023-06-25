import pytest

from uscensus.incremental import filters, model, query, wrappers


def test_tab_query_builder_on_non_microdata_raises(httpx_client_full):
    catalog = wrappers.Catalog.get_catalog(httpx_client_full)
    datasets = filters.filter_datasets(
        catalog.dataset,
        vintages=[2022],
        title='Planning')
    ds = datasets[1]

    with pytest.raises(TypeError) as exc_info:
        query.TabulationQueryBuilder(ds)
    assert 'non-microdata' in str(exc_info.value)


def test_query_builder(httpx_client_full):
    catalog = wrappers.Catalog.get_catalog(httpx_client_full)
    datasets = filters.filter_datasets(
        catalog.dataset,
        vintages=[2022],
        title='Planning')
    ds = datasets[1]

    df = query.QueryBuilder(
        ds,
    ).set_fields(
        'pct_US_Cit_Nat_ACSMOE_16_20',
    ).set_geo_for(
        'tract', '*',
    ).add_geo_in(
        'state', 36,
    ).query()

    assert df.shape == (5411, 4)
    assert all(df.columns == [
               'pct_US_Cit_Nat_ACSMOE_16_20', 'state', 'county', 'tract'])


def test_query_builder_unknown_field_raises(httpx_client_full):
    catalog = wrappers.Catalog.get_catalog(httpx_client_full)
    datasets = filters.filter_datasets(
        catalog.dataset,
        vintages=[2022],
        title='Planning')
    ds = datasets[1]

    with pytest.raises(ValueError) as exc_info:
        query.QueryBuilder(
            ds,
        ).set_fields(
            'INVALID_FIELD',
        )
    assert 'Unknown field' in str(exc_info.value)


def test_query_builder_pred_only_field_raises(httpx_client_full):
    catalog = wrappers.Catalog.get_catalog(httpx_client_full)
    datasets = filters.filter_datasets(
        catalog.dataset,
        vintages=[2022],
        title='Planning')
    ds = datasets[1]

    with pytest.raises(ValueError) as exc_info:
        query.QueryBuilder(
            ds,
        ).set_fields(
            'ucgid',
        )
    assert 'predicate-only' in str(exc_info.value)


@pytest.mark.parametrize('udata_httpx_client',
                         ('query-results-tabulated-weighted',),
                         indirect=True)
def test_query_builder_no_weight_raises(udata_httpx_client):
    catalog = wrappers.Catalog.get_catalog(udata_httpx_client,
                                           catalog_subpath='data/1989/cps/basic/apr')
    ds = catalog.dataset[0]

    with pytest.raises(ValueError) as exc_info:
        query.QueryBuilder(
            ds,
        ).set_fields(
            'A_AGE',
        )
    assert 'weights not requested for microdata' in str(exc_info.value)


def test_query_builder_invalid_group_raises(httpx_client_full):
    catalog = wrappers.Catalog.get_catalog(httpx_client_full)
    datasets = filters.filter_datasets(
        catalog.dataset,
        vintages=[2022],
        title='Planning')
    ds = datasets[1]

    with pytest.raises(ValueError) as exc_info:
        query.QueryBuilder(
            ds,
        ).set_groups(
            'INVALID_GROUP',
        )
    assert 'Unknown group' in str(exc_info.value)


def test_query_builder_invalid_predicate_raises(httpx_client_full):
    catalog = wrappers.Catalog.get_catalog(httpx_client_full)
    datasets = filters.filter_datasets(
        catalog.dataset,
        vintages=[2022],
        title='Planning')
    ds = datasets[1]

    with pytest.raises(ValueError) as exc_info:
        query.QueryBuilder(
            ds,
        ).add_predicate('INVALID', '*')
    assert 'Unknown predicate' in str(exc_info.value)


def test_query_builder_bad_string_predicate_raises(httpx_client_full):
    catalog = wrappers.Catalog.get_catalog(httpx_client_full)
    datasets = filters.filter_datasets(
        catalog.dataset,
        vintages=[2022],
        title='Planning')
    ds = datasets[1]

    class BadType:
        pass

    with pytest.raises(TypeError) as exc_info:
        query.QueryBuilder(
            ds,
        ).add_predicate('Med_HHD_Inc_ACS_16_20', BadType())
    assert 'requires str value' in str(exc_info.value)


def test_query_builder_bad_int_predicate_raises(httpx_client_full):
    catalog = wrappers.Catalog.get_catalog(httpx_client_full)
    datasets = filters.filter_datasets(
        catalog.dataset,
        vintages=[2022],
        title='Planning')
    ds = datasets[1]

    with pytest.raises(TypeError) as exc_info:
        query.QueryBuilder(
            ds,
        ).add_predicate('Tot_Occp_Units_ACSMOE_16_20', 'INVALID')
    assert 'requires int value' in str(exc_info.value)


def test_query_builder_non_geo_for_raises(httpx_client_full):
    catalog = wrappers.Catalog.get_catalog(httpx_client_full)
    datasets = filters.filter_datasets(
        catalog.dataset,
        vintages=[2022],
        title='Planning')
    ds = datasets[1]

    with pytest.raises(ValueError) as exc_info:
        query.QueryBuilder(
            ds,
        ).add_predicate('for', '*')
    assert 'using set_geo_* methods' in str(exc_info.value)


def test_query_builder_non_geo_in_raises(httpx_client_full):
    catalog = wrappers.Catalog.get_catalog(httpx_client_full)
    datasets = filters.filter_datasets(
        catalog.dataset,
        vintages=[2022],
        title='Planning')
    ds = datasets[1]

    with pytest.raises(ValueError) as exc_info:
        query.QueryBuilder(
            ds,
        ).add_predicate('in', '*')
    assert 'using set_geo_* methods' in str(exc_info.value)


def test_query_builder_missing_geo_for_raises(httpx_client_full):
    catalog = wrappers.Catalog.get_catalog(httpx_client_full)
    datasets = filters.filter_datasets(
        catalog.dataset,
        vintages=[2022],
        title='Planning')
    ds = datasets[1]

    with pytest.raises(ValueError) as exc_info:
        query.QueryBuilder(
            ds,
        ).query()
    assert 'Geography is required' in str(exc_info.value)


def test_query_builder_bad_geo_for_raises(httpx_client_full):
    catalog = wrappers.Catalog.get_catalog(httpx_client_full)
    datasets = filters.filter_datasets(
        catalog.dataset,
        vintages=[2022],
        title='Planning')
    ds = datasets[1]

    with pytest.raises(ValueError) as exc_info:
        query.QueryBuilder(
            ds,
        ).set_geo_for(
            'INVALID', '*',
        ).query()
    assert 'Invalid "for" geography' in str(exc_info.value)


def test_query_builder_missing_required_raises(httpx_client_full):
    catalog = wrappers.Catalog.get_catalog(httpx_client_full)
    datasets = filters.filter_datasets(
        catalog.dataset,
        vintages=[2022],
        title='Planning')
    ds = datasets[1]

    ds.variables['required'] = model.Variable(label='required', required=True)

    with pytest.raises(ValueError) as exc_info:
        query.QueryBuilder(
            ds,
        ).set_geo_for(
            'tract', '*',
        ).query()
    assert 'Missing required' in str(exc_info.value)


@pytest.mark.parametrize('udata_httpx_client',
                         ('query-results-tabulated-weighted',),
                         indirect=True)
def test_udata_query_builder_weighted(udata_httpx_client):
    catalog = wrappers.Catalog.get_catalog(udata_httpx_client,
                                           catalog_subpath='data/1989/cps/basic/apr')
    ds = catalog.dataset[0]
    tqb = query.TabulationQueryBuilder(
        ds,
    ).set_weight(
        'A_FNLWGT',
    ).set_rows(
        'A_MARITL',
    ).set_cols(
        'A_LFSR',
    ).add_predicate(
        'A_HGA', 12,
    )
    df = tqb.query()

    assert df.columns.names == tqb.cols
    assert df.index.names == tqb.rows


@pytest.mark.parametrize('udata_httpx_client',
                         ('query-results-tabulated-unweighted',),
                         indirect=True)
def test_udata_query_builder_unweighted(udata_httpx_client):
    catalog = wrappers.Catalog.get_catalog(udata_httpx_client,
                                           catalog_subpath='data/1989/cps/basic/apr')
    ds = catalog.dataset[0]
    tqb = query.TabulationQueryBuilder(
        ds,
    ).set_rows(
        'A_MARITL',
    ).set_cols(
        'A_LFSR',
    ).add_predicate(
        'A_HGA', 12,
    )
    df = tqb.query()

    assert df.columns.names == tqb.cols
    assert df.index.names == tqb.rows


@pytest.mark.parametrize('udata_httpx_client',
                         ('query-results-tabulated-weighted-avg',),
                         indirect=True)
def test_udata_query_builder_weighted_avg(udata_httpx_client):
    catalog = wrappers.Catalog.get_catalog(udata_httpx_client,
                                           catalog_subpath='data/1989/cps/basic/apr')
    ds = catalog.dataset[0]
    tqb = query.TabulationQueryBuilder(
        ds,
    ).set_weight(
        'A_FNLWGT',
    ).set_avg(
        'A_AGE',
    ).set_rows(
        'A_MARITL',
    ).set_cols(
        'A_LFSR',
    ).add_predicate(
        'A_HGA', 12,
    )
    df = tqb.query()

    assert df.columns.names == [*tqb.cols, 'A_AGE']
    assert df.index.names == tqb.rows


@pytest.mark.parametrize('udata_httpx_client',
                         ('query-results-tabulated-unweighted-avg',),
                         indirect=True)
def test_udata_query_builder_unweighted_avg(udata_httpx_client):
    catalog = wrappers.Catalog.get_catalog(udata_httpx_client,
                                           catalog_subpath='data/1989/cps/basic/apr')
    ds = catalog.dataset[0]
    tqb = query.TabulationQueryBuilder(
        ds,
    ).set_avg(
        'A_AGE',
    ).set_rows(
        'A_MARITL',
    ).set_cols(
        'A_LFSR',
    ).add_predicate(
        'A_HGA', 12,
    )
    df = tqb.query()

    assert df.columns.names == [*tqb.cols, 'A_AGE']
    assert df.index.names == tqb.rows


@pytest.mark.parametrize('udata_httpx_client',
                         ('query-results-tabulated-weighted-nocols',),
                         indirect=True)
def test_udata_query_builder_weighted_nocols(udata_httpx_client):
    catalog = wrappers.Catalog.get_catalog(udata_httpx_client,
                                           catalog_subpath='data/1989/cps/basic/apr')
    ds = catalog.dataset[0]
    tqb = query.TabulationQueryBuilder(
        ds,
    ).set_weight(
        'A_FNLWGT',
    ).set_rows(
        'A_MARITL', 'A_LFSR',
    ).add_predicate(
        'A_HGA', 12,
    )
    df = tqb.query()

    assert df.columns.names == [None]
    assert df.index.names == tqb.rows


@pytest.mark.parametrize('udata_httpx_client',
                         ('query-results-tabulated-weighted-norows',),
                         indirect=True)
def test_udata_query_builder_weighted_norow(udata_httpx_client):
    catalog = wrappers.Catalog.get_catalog(udata_httpx_client,
                                           catalog_subpath='data/1989/cps/basic/apr')
    ds = catalog.dataset[0]
    tqb = query.TabulationQueryBuilder(
        ds,
    ).set_weight(
        'A_FNLWGT',
    ).set_cols(
        'A_MARITL', 'A_LFSR',
    ).add_predicate(
        'A_HGA', 12,
    )
    df = tqb.query()

    assert df.columns.names == tqb.cols
    assert df.index.names == [None]


@pytest.mark.parametrize('udata_httpx_client',
                         ('query-results-tabulated-weighted',),
                         indirect=True)
def test_udata_query_builder_raises_unknown_weight(udata_httpx_client):
    catalog = wrappers.Catalog.get_catalog(udata_httpx_client,
                                           catalog_subpath='data/1989/cps/basic/apr')
    ds = catalog.dataset[0]
    with pytest.raises(ValueError) as exc_info:
        query.TabulationQueryBuilder(
            ds,
        ).set_weight(
            'INVALID_FIELD',
        )
    assert 'Unknown field' in str(exc_info.value)


@pytest.mark.parametrize('udata_httpx_client',
                         ('query-results-tabulated-weighted',),
                         indirect=True)
def test_udata_query_builder_raises_bad_weight(udata_httpx_client):
    catalog = wrappers.Catalog.get_catalog(udata_httpx_client,
                                           catalog_subpath='data/1989/cps/basic/apr')
    ds = catalog.dataset[0]
    with pytest.raises(ValueError) as exc_info:
        query.TabulationQueryBuilder(
            ds,
        ).set_weight(
            'A_AGE',
        )
    assert 'not a weight variable' in str(exc_info.value)


@pytest.mark.parametrize('udata_httpx_client',
                         ('query-results-tabulated-weighted',),
                         indirect=True)
def test_udata_query_builder_raises_unknown_avg(udata_httpx_client):
    catalog = wrappers.Catalog.get_catalog(udata_httpx_client,
                                           catalog_subpath='data/1989/cps/basic/apr')
    ds = catalog.dataset[0]
    with pytest.raises(ValueError) as exc_info:
        query.TabulationQueryBuilder(
            ds,
        ).set_avg(
            'INVALID_FIELD',
        )
    assert 'Unknown field' in str(exc_info.value)


@pytest.mark.parametrize('udata_httpx_client',
                         ('query-results-tabulated-weighted',),
                         indirect=True)
def test_udata_query_builder_raises_categorical_avg(udata_httpx_client):
    catalog = wrappers.Catalog.get_catalog(udata_httpx_client,
                                           catalog_subpath='data/1989/cps/basic/apr')
    ds = catalog.dataset[0]
    with pytest.raises(ValueError) as exc_info:
        query.TabulationQueryBuilder(
            ds,
        ).set_avg(
            'HG_FIPS',
        )
    assert 'average of categorical' in str(exc_info.value)


@pytest.mark.parametrize('udata_httpx_client',
                         ('query-results-tabulated-weighted',),
                         indirect=True)
def test_udata_query_builder_raises_bad_row(udata_httpx_client):
    catalog = wrappers.Catalog.get_catalog(udata_httpx_client,
                                           catalog_subpath='data/1989/cps/basic/apr')
    ds = catalog.dataset[0]
    with pytest.raises(ValueError) as exc_info:
        query.TabulationQueryBuilder(
            ds,
        ).set_rows(
            'INVALID_FIELD',
        )
    assert 'Unknown field' in str(exc_info.value)


@pytest.mark.parametrize('udata_httpx_client',
                         ('query-results-tabulated-weighted',),
                         indirect=True)
def test_udata_query_builder_raises_bad_col(udata_httpx_client):
    catalog = wrappers.Catalog.get_catalog(udata_httpx_client,
                                           catalog_subpath='data/1989/cps/basic/apr')
    ds = catalog.dataset[0]
    with pytest.raises(ValueError) as exc_info:
        query.TabulationQueryBuilder(
            ds,
        ).set_cols(
            'INVALID_FIELD',
        )
    assert 'Unknown field' in str(exc_info.value)


@pytest.mark.parametrize('udata_httpx_client',
                         ('query-results-tabulated-weighted',),
                         indirect=True)
def test_udata_query_builder_raises_bad_recode_base(udata_httpx_client):
    catalog = wrappers.Catalog.get_catalog(udata_httpx_client,
                                           catalog_subpath='data/1989/cps/basic/apr')
    ds = catalog.dataset[0]
    with pytest.raises(ValueError) as exc_info:
        query.TabulationQueryBuilder(
            ds,
        ).add_recode(
            'RECODED_VAR', 'INVALID_FIELD', [],
        )
    assert 'Unknown field' in str(exc_info.value)


@pytest.mark.parametrize('udata_httpx_client',
                         ('query-results-tabulated-weighted',),
                         indirect=True)
def test_udata_query_builder_raises_bad_recode_value(udata_httpx_client):
    catalog = wrappers.Catalog.get_catalog(udata_httpx_client,
                                           catalog_subpath='data/1989/cps/basic/apr')
    ds = catalog.dataset[0]
    with pytest.raises(ValueError) as exc_info:
        query.TabulationQueryBuilder(
            ds,
        ).add_recode(
            'RECODED_VAR', 'HG_FIPS', [query.RecodeRange(25, 100)],
        )
    assert 'Invalid recode value' in str(exc_info.value)


@pytest.mark.parametrize('udata_httpx_client',
                         ('query-results-tabulated-weighted',),
                         indirect=True)
def test_udata_query_builder_recode_range_ok(udata_httpx_client):
    catalog = wrappers.Catalog.get_catalog(udata_httpx_client,
                                           catalog_subpath='data/1989/cps/basic/apr')
    recode_range = query.RecodeRange(1, 5)
    ds = catalog.dataset[0]
    tqb = query.TabulationQueryBuilder(
        ds,
    ).add_recode(
        'RECODED_VAR', 'A_UNTYPE', [recode_range],
    )
    assert tqb.recodes['RECODED_VAR'].b == 'A_UNTYPE'
    assert tqb.recodes['RECODED_VAR'].d == [[recode_range]]


@pytest.mark.parametrize('udata_httpx_client',
                         ('query-results-tabulated-weighted',),
                         indirect=True)
def test_udata_query_builder_recode_col_ok(udata_httpx_client):
    catalog = wrappers.Catalog.get_catalog(udata_httpx_client,
                                           catalog_subpath='data/1989/cps/basic/apr')
    recode_range = query.RecodeRange(1, 5)
    ds = catalog.dataset[0]
    tqb = query.TabulationQueryBuilder(
        ds,
    ).add_recode(
        'RECODED_VAR', 'A_UNTYPE', [-1], [recode_range],
    ).set_cols(
        'RECODED_VAR',
    )
    assert tqb.cols == ['RECODED_VAR']
