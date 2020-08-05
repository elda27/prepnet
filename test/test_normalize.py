import pytest
import pandas as pd
import numpy as np

from prepnet.normalize.quantile_normalize import QuantileNormalize
from prepnet.normalize.standardize import Standardize

@pytest.mark.parametrize(('input', 'expected', 'quantile'), [
    (
        [10.0, 200.0, 40.0, 20.0, 15.0, 18.0],
        [15.0, 40.0, 40.0, 20.0, 15.0, 18.0],
        0.8
    )
])
def test_quantile_normalize(input, expected, quantile):
    input_series = pd.Series(input)
    expected_series = pd.Series(expected)

    converter = QuantileNormalize(quantile)
    output_series = converter.encode(input_series)
    pd.testing.assert_series_equal(expected_series, output_series)

    reconstruct_series = converter.decode(output_series)
    pd.testing.assert_series_equal(input_series, reconstruct_series)


@pytest.mark.parametrize(('mu', 'sigma'), [
    (20.0, 1.0),
    (1.0, 7.0),
    (4.0, 10.0),
])
def test_standardise(mu, sigma):
    input_series = pd.Series(np.random.normal(mu, sigma, size=(100000,)))
    
    converter = Standardize()
    output_series = converter.encode(input_series)
    out_mu = np.mean(output_series)
    out_sigma = np.std(output_series)
    assert out_mu == pytest.approx(0, rel=1e-1) and out_sigma == pytest.approx(1, rel=1e-1)

    reconstruct_series = converter.decode(output_series)
    reconstruct_mu = np.mean(reconstruct_series)
    reconstruct_sigma = np.std(reconstruct_series)
    assert reconstruct_mu == pytest.approx(mu, rel=1e-1) and reconstruct_sigma == pytest.approx(sigma, rel=1e-1)