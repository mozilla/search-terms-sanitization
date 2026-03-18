import pyarrow as pa
import pytest
from sanitation_job import _prefetch_iterator


def test_empty_iterator_does_not_raise():
    """Test that an empty iterable should yield nothing."""
    results = list(_prefetch_iterator(iter([])))
    assert results == []


def test_single_batch_smaller_than_batch_size():
    """Test that a batch smaller than the minimal size is still returned."""
    batch = pa.RecordBatch.from_pydict({"x": [1, 2, 3]})
    results = list(_prefetch_iterator(iter([batch]), batch_size=10))
    assert len(results) == 1
    assert results[0].num_rows == 3


def test_exact_multiple_of_batch_size():
    """Test that when the iterable length is an exact multiple of batch_size,
    the final pull hits the sentinel with an empty batch.
    This checks an issue from DENG-10815 where we would try to pass an empty list to pyarrow.concat_batches."""
    batches = [pa.RecordBatch.from_pydict({"x": [i]}) for i in range(4)]
    results = list(_prefetch_iterator(iter(batches), batch_size=2))
    assert len(results) == 2
    assert results[0].num_rows == 2
    assert results[1].num_rows == 2
